#!/usr/bin/python3
# Updates cadia data from zonefiles
# Supports incremental updates by comparing previous zone file, only submits the changed domains
# To test: python3 zonefile.py --tld=com --date=2022-08-22 --dir=tests/data --noop
import argparse
import datetime
import gzip
import logging
import os.path
from utils import kafka_result_producer, normalize, clickhouse_conn
from result import ZonefileResult

# Good enough zonefile line parser. Taken from "common/bin/zonfileimport.py"
# Returns domain, dns
def process_line(line,tld):
    col = line.decode().lower().split()
    if not col or len(col) < 3:
        # empty line
        return None,None
    if col[0] == tld + '.':
        # skip SOA record
        return None,None
    domain = col[0]
    rtype = col[-2]
    dns = col[-1]
    #print(rtype)
    #print(line)
    #print(col)

    if rtype.lower() != 'ns':
        #print("Ignoring record with type %s: %s" % (rtype, col))
        return None,None

    if dns == 'rrsig':
        # dnssec records, not parsed correctly but matches the 'ns' above, skip
        return None,None

    if dns.endswith('.'):
        dns = dns.rstrip('.')
    else:
        dns = dns + '.' + tld

    if domain.endswith('.'):
        domain = domain.rstrip('.')
    else:
        domain = domain + '.' + tld

    #if domain == 'trellian.com':
    #    print(line)
    #    print(col)
    #    print("domain: %s rtype %s dns %s" % (domain,rtype,col))

    return [domain,dns]

# Parses the given file. Yields (domain,[ns,..])
def parse_zone_file(filename,tld):
    with gzip.open(filename) as f:
        lines = 0
        last_domain = None
        collect_dns = []
        for line in f:
            lines += 1
            if lines % 1000000 == 0:
                log.debug("%s - %d lines" % (filename,lines))
            try:
                domain,dns = process_line(line,tld)
            except IndexError:
                #raise ValueError("Unable to parse line %d: %s" % (lines, line))
                # TODO count parsing errors and raise an exception if too many
                print("Skipping unparseable line %d: %s" % (lines, line))
                continue
            if domain is None:
                continue
            if last_domain is None:
                last_domain = domain
            if last_domain != domain:
                # next domain. output result
                yield(last_domain, sorted(collect_dns))
                last_domain = domain
                collect_dns = []
            collect_dns.append(dns)
        #log.info("EOF on %s" % filename)
        yield(last_domain, sorted(collect_dns)) # last domain

# Parses both zone files and returns only changed domains
# Assumes the files are sorted by domain, which is the case for every TLD zone we've seen
# UPDATE: this assumption did not hold. This approach seems unusable
# Uses iterators to avoid reading the whole contents into memory. These files can be large
# This function was renamed and is unused. Retained incase we find it better for some TLDs
def read_zonefile_incremental_iter(filename,prev_filename,tld):
    old = parse_zone_file(prev_filename,tld)
    cur = parse_zone_file(filename,tld)

    old_domain,old_dns = next(old)
    cur_domain,cur_dns = next(cur)
    try:
        while True:
            #print(old_domain,cur_domain)
            if old_domain == cur_domain:
                # Same domain, compare DNS
                if old_dns != cur_dns:
                    yield(cur_domain,sorted(cur_dns))
                # advance both files
                old_domain,old_dns = next(old)
                cur_domain,cur_dns = next(cur)
            if cur_domain > old_domain:
                # cur_domain is greater, means old_domain is absent
                yield(old_domain,[])
                # advance old file only
                old_domain,old_dns = next(old)
            if cur_domain < old_domain:
                yield(cur_domain,sorted(cur_dns))
                # advance current file only
                cur_domain,cur_dns = next(cur)
    except StopIteration:
        # hit the end of one file. could be the old file. send the current line, then parse the remainder of current file
        # this can cause a duplicate, no big deal for this use case
        yield(cur_domain,cur_dns)
        for cur_domain,cur_dns in cur:
            yield(cur_domain,cur_dns)

# Parses both zone files and returns only changed domains
# Returns an iterator which yields: domain, [ns1, ns2 ...]
def read_zonefile_incremental(filename,prev_filename,tld):
    # Replacing ns (list of str) to an int to reduce memory requirements. Lists can be large eg .com 170m rows, doesn't fit in 64gb ram
    # This is significantly slower (about 13m for com) but avoids spark etc. May revisit later
    ns_id = {} # [ns_as_string] => id
    nextid = 0 # counter
    old = {}
    cur = {}
    log.debug("Read prev zone...")
    for dom, ns in parse_zone_file(prev_filename,tld):
        ns = tuple(ns) # can use tuple as key, can't use list
        if ns in ns_id:
            old[dom] = ns_id[ns]
        else:
            ns_id[ns] = nextid
            old[dom] = nextid
            nextid += 1
    log.debug("Read new zone...")
    for dom, ns in parse_zone_file(filename,tld):
        ns = tuple(ns) # can use tuple as key, can't use list
        if ns in ns_id:
            cur[dom] = ns_id[ns]
        else:
            ns_id[ns] = nextid
            cur[dom] = nextid
            nextid += 1
    log.debug("Done reading zones")

    log.debug("Finding changes...")
    new_or_changed = { dom : ns_id  for dom,ns_id in cur.items() if not dom in old or old[dom] != cur[dom] }
    removed = ( dom for dom in old.keys() if not dom in cur )

    log.debug("Invert dict...")
    ns_values = { v : k for k, v in ns_id.items() } # invert dict

    log.debug("Return new and changed domains...")
    for dom,ns_id in new_or_changed.items():
        yield dom,list(ns_values[ns_id])
    log.debug("Return removed domains...")
    for dom in removed:
        yield dom, []
    log.debug("read_zonefile_incremental done!")

def find_previous_zonefile(dir, tld, date):
    for i in range(1,7):
        filename = "%s/zone-%s-%s.gz" % (dir, tld, date - datetime.timedelta(days=i))
        if os.path.exists(filename):
            return filename

def send_results(changes, tld, date):
    timestamp = datetime.datetime.combine(date, datetime.time())
    # send to kafka
    producer = kafka_result_producer()
    common = { 'modified_by' : 'zonefile', 'zonefile_date' : date, 'last_modified' : timestamp }
    for line in changes:
        if line[1]:
            ns_domains = sorted(list(filter(lambda x: x is not None, set([normalize(n) for n in line[1]]))))
        else:
            ns_domains = []
        producer.send('results', key=line[0], value=ZonefileResult(**{'domain' : line[0], 'ns' : [ n for n in line[1] ], 'ns_domains' : ns_domains, 'tld' : tld.encode().decode('idna'), **common }) )
        #log.debug({'domain' : line[0], 'ns' : line[1], 'ns_domains' : ns_domains})
    producer.flush()

def record_completed(tld,date):
    clickhouse_conn().execute("insert into zonefile_imported (tld,date) VALUES", ((tld, date),), types_check=True)

def import_zonefile(tld, date, dir='/var/lib/incoming/tldzones', full=False, noop=False):
    filename = "%s/zone-%s-%s.gz" % (dir, tld, date)
    prev_filename = find_previous_zonefile(dir, tld, date)
    if prev_filename and not full:
        log.info("Incremental update since %s" % prev_filename)
        if tld == 'com':
            # .com is huge. this method uses less RAM. works for .com because the zone files are sorted
            changes = read_zonefile_incremental_iter(filename,prev_filename,tld)
        else:
            changes = read_zonefile_incremental(filename,prev_filename,tld)
    else:
        log.info("Full update")
        changes = parse_zone_file(filename,tld)

    if not full:
        # fetch all from iterator before starting, so we don't send partial results when parsing fails
        # skipping on full import because .com is too big, and we can easily clean up
        changes = list(changes)

    log.info("Finished reading. Sending results...")
    if noop:
        results = list(changes) # changes can be an iterator, this makes it actually evaluate
        log.info("No-op mode. There were %d changes" % len(results))
    else:
        send_results(changes, tld, date)
        record_completed(tld,date)

    log.info("Finished importing for tld %s" % tld)

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger('log')
log.setLevel(logging.INFO)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import zone file')
    parser.add_argument('--tld', required=True, type=lambda s: s.split(','), help='Top level domain. One or more, comma separated')
    parser.add_argument('--date', required=True, type=datetime.date.fromisoformat, help='Date to import for')
    parser.add_argument('--dir', default='/var/lib/incoming/tldzones', help='Directory containing zone files')
    parser.add_argument('--full', action='store_true', help='Import full file, not incremental from previous file')
    parser.add_argument('--noop', action='store_true', help='No-op. Skip sending results')
    parser.add_argument('--debug', action='store_true', help='Set logging level to debug')
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    for tld in args.tld:
        import_zonefile(tld, args.date, args.dir, args.full, args.noop)
