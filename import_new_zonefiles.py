#!/usr/bin/python3
# Compares the zonefiles directory and currently imported zonefiles to find any requiring import
import argparse
import concurrent.futures
import datetime
import re
import traceback
from os import listdir
from subprocess import run, CalledProcessError
from collections import defaultdict
from utils import clickhouse_conn
from zonefile import import_zonefile
from trellian.job import add_job


# returns dict: zone => [ date1, ... daten ]
def list_all_zonefiles():
    print("Listing all zone files...")
    all_zonefiles = defaultdict(list)
    path = '/var/lib/incoming/tldzones'
    for filename in listdir(path):
        m = re.match('zone-([a-z0-9\.\-]+)-(\d\d\d\d-\d\d-\d\d).gz', filename)
        if m:
            all_zonefiles[m[1]].append(m[2])
        else:
            print("WARNING: filename doesn't match regex: %s" % filename)
    return all_zonefiles

def latest_imported_zonefiles():
    # Tracking this in a separate table instead of querying "domains" because some TLDs go months/years without any update, so this avoids parsing them every day
    ch = clickhouse_conn()
    rows = ch.execute("select tld,max(date) from zonefile_imported group by tld")
    return { r[0] : str(r[1]) for r in rows }

# import one or more days for one tld
def import_zones(tld, dates):
    for date in dates:
        print("Import %s %s..." % (tld, date))
        try:
            import_zonefile(tld, date)
        except Exception as e:
            # Wrap the exception to include useful details for debugging
            raise RuntimeError("Failed to import for tld %s date %s" % (tld,date)) from e

        print("Finished %s %s" % (tld, date))

parser = argparse.ArgumentParser(description='Import all new zone files')
parser.add_argument('--tld', required=False, type=lambda s: s.split(','), help='Top level domain. One or more, comma separated')
parser.add_argument('--exclude-tlds', required=False, type=lambda s: s.split(','), default=[], help='TLDs to exclude. One or more, comma separated')
parser.add_argument('--noop', action='store_true', help='No-op')
args = parser.parse_args()

all_zonefiles = list_all_zonefiles()
latest_imported_zonefiles = latest_imported_zonefiles()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
futures = []

tlds = args.tld if args.tld else all_zonefiles.keys()
tlds = [ t for t in tlds if t not in args.exclude_tlds ]

for tld in tlds:
    #if tld in latest_imported_zonefiles:
    imported_date = latest_imported_zonefiles[tld] if tld in latest_imported_zonefiles else '2024-02-17' # '2019-04-24' # max 5 years

    #print("TLD %s imported_date %s" % (tld, imported_date))
    dates = sorted([ datetime.date.fromisoformat(d) for d in all_zonefiles[tld] if d > imported_date])
    if not dates:
        continue
    futures.append(executor.submit(import_zones, tld, dates ))

print("Waiting for executor to finish...")
concurrent.futures.wait(futures)
executor.shutdown()
failures = [ f for f in futures if f.exception() ]
if failures:
    print("FAILURES:")
    for f in failures:
        traceback.print_exception(f.exception())
print("Finished")
exit(1 if failures else 0)
