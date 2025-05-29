#!/usr/bin/python3
# Updates the cadia DB
# Consumes from the results queue in kafka and writes to clickhouse
# Also can queue further scrapes, eg after a domain is updated from the zonefile we re-scrape dns
#
from datetime import datetime, timedelta
from utils import kafka_consumer, clickhouse_conn, queue_scrape, db_cols, db_default_row, tld
from result import Result
import maxminddb
import atexit
from typing import List
from pprint import pprint

ch = ch2 = cols = cols_str = None



# define the path to the GeoIP2-City.mmdb and GeoIP2-ISP.mmdb files
geo_path = '/var/state/www-local/maxminddb/GeoIP2-City.mmdb' 
asn_path = '/var/state/www-local/maxminddb/GeoIP2-ISP.mmdb'

# open the GeoIP2-City.mmdb and GeoIP2-ISP.mmdb files
geo_reader = maxminddb.open_database(geo_path) 
asn_reader = maxminddb.open_database(asn_path)


def format_list(items):
    return "'" + "','".join(items) + "'" # TODO SQLi

# Updates is list of Result objects
#
# Clickhouse doesn't directly support updates. This uses the ReplacingMergeTree engine
# to replace previous rows for the same primary key. The update request won't contain
# all columns, so we need to fetch the current value first and merge it
#
# TODO verify that this gives correct results if the same domain is updated multiple times in the same batch - seems not- gets inserted multiple times with latest value
# TODO schema enforcement
# TODO error handling - record failed rows
def merge_rows(updates):
    domains = [ u.domain for u in updates ]
    sql = 'select %s from domains where domain in (%s)' % (cols_str, format_list(domains))
    ch2.execute("set max_query_size=524288") # set first the max query size
    #print("Loggin the sql:", sql)
    result = ch2.execute(sql) # then execute
    oldrows = { r[0]: dict(zip(cols, r)) for r in result } # dict of domain => { col:value, ... }
    empty_row = db_default_row()
    for update in updates:
        domain = update.domain
        try:
            row = oldrows[domain].copy() # copy because we may have multiple changes for the same domain, don't overwrite object
        except KeyError:
            # Domain was not found in clickhouse. Start with empty row
            row = empty_row.copy()
        old = row.copy()
        row.update(update.asdict())
        try:
            enrich(old, row)
        except ValueError as e:
            print(e)
            # TODO do something better with these errors. for now skip so they don't block the queue
            continue
        oldrows[domain] = row # incase same domain is updated twice, to avoid losing first update
        #print(row) # temporary for debugging
        yield(row)


def enrich(old, new):
    # create a private function for the geo and asn lookups
    def _look_up_geo(addr: List[str]):
        ip_addresses = list(set(addr))
        iso_codes = []
        for ip in ip_addresses:
            result = geo_reader.get(ip)
            try:
                iso_codes.append(result['country']['iso_code'])
            except (KeyError, TypeError):
                pass     
        return sorted(list(set(iso_codes)))  # make the iso_codes set sorted 
    
    
    def _look_up_asn(addr: List[str]):
        ip_addresses = list(set(addr))
        asns = []
        for ip in ip_addresses:
            result = asn_reader.get(ip)
            try:
                asns.append(str(result['autonomous_system_number']))
            except (KeyError, TypeError):
                pass

        return sorted(list(set((asns))))

    # create a private function to close the readers when the program exits
    def _close_readers():
        geo_reader.close()

    # Register the close_readers function to be called on program exit
    # avoids memory leaks
    atexit.register(_close_readers)

    # Do various data enrichment in here to keep this separate from the db logic
    # This is called once per row before a row is updated. `old` and `new` are the existing and updated row contents
    # This can modify `new` in place to change what will be written to the db
    
    if new['modified_by'] == 'dns' and new['dns_exists'] and not new['registered']:
        # if it resolves it must be registered
        new['registered'] = True
        
    # ASN and geo
    if new['modified_by'] == 'dns' and new['addr']:
        new['geo'] = _look_up_geo(new['addr'])
        new['asn'] = _look_up_asn(new['addr'])
        
    # Add TLD if missing
    if not 'tld' in new:
        t = tld(new['domain'])
        if t is None:
            raise ValueError(f"Unknown TLD for domain: {new['domain']}")
        new['tld'] = t

    # TODO we could also trigger scrapes here, eg
    # - scrape dns after zonefile results
    # - scrape rdap when dns changes
    # - detect when status changed and trigger something
    # eg if new['modified_by'] == 'dns' and new['addr'] != old['addr']:

    # Allow None or datetime, raise error for anything else
    if new["expiration_date"] is not None and not isinstance(new["expiration_date"], datetime):
        raise ValueError(f"Invalid expiration_date for {new['domain']}: {new['expiration_date']}")

def upsert(rows):
    #print("Inserting rows:")
    #pprint(rows)
    ch.execute("insert into domains (%s) VALUES" % cols_str,
        merge_rows(rows), types_check=True
    )

# Batching logic - up to x records, or domain length limit, or consumer timeout (to flush when idle)
def batched_consumer():
    consumer = kafka_consumer('results', 'update', consumer_timeout_ms=1000)
    updates = []
    count = domainlen = 0
    while True:
        print("Waiting for messages...")
        for msg in consumer:
            #print("Got a message!")
            domain = msg.key
            try:
                row = Result.fromdict(msg.value.copy())
            except (KeyError, ValueError) as e:
                # TODO write to some queue or table
                print("Bad result: %s" % e)
                continue
            row.domain = domain
            updates.append(row)
            count += 1
            domainlen += len(domain) + 3 # +3 is for quotes and comma
            if domainlen >= 80000: # max_query_size=512KB, can reach for select query
                yield(updates)
                # TODO commit?
                updates = []
                domainlen = 0
        # Loop will break if we reach consumer_timeout_ms. Flush.
        if updates:
            yield(updates)
            updates = []
            domainlen = 0
            # TODO commit?

# trigger any follow-up scrapes. Eg after we find a domain changed in the zonefile we will re-scrape dns
# updates is an array of Result (or subclasses of it eg DNSResult)
#
# Note that this is called ever time a result is submitted, not only when the result is different to the previous
# Further work is needed to trigger only for actual changes. The whois scraper already sends only modified rows
# so it works fine for that.
def trigger_scrapes(updates):
    for update in updates:
        if update.modified_by == 'zonefile':
            queue_scrape(update.domain, 'dns')
            queue_scrape(update.domain, 'whois')

def main():
    global ch, ch2, cols, cols_str

    ch = clickhouse_conn()
    ch2 = clickhouse_conn() # second connection to fetch rows while first is blocked doing insert

    # Get list of columns
    cols = db_cols().keys()
    cols_str = ','.join(cols)

    # Consume from kafka
    count = 0
    start_wait = datetime.now()
    start_date = datetime.now() # will use to check the start date running the script
    three_days_duration = timedelta(days=3)
    for updates in batched_consumer():
        start = datetime.now()
        #merged = merge_rows(updates)
        #print([ u for u in merged if 'zonefile_date' in u and type(u['zonefile_date']).__name__ == 'str' ])

        #ch.execute("insert into domains (%s) VALUES" % cols_str,
        #    merge_rows(updates), types_check=True
        #)
        upsert(updates)
        trigger_scrapes(updates)
        duration = (datetime.now() - start).total_seconds()
        wait_duration = (datetime.now() - start_wait).total_seconds() - duration
        count += len(updates)
        print("Done %d domains in %0.1f seconds, average %d domains/sec. %0.1f seconds wait. %d domains total since start" % (len(updates),duration,len(updates)/duration,wait_duration,count))
        now = start_wait = datetime.now()
        if now - start_date > three_days_duration:
            print("3 days passed. Terminating script")
            exit() # terminate the script

if __name__ == '__main__':
    main()
