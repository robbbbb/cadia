import argparse
from utils import clickhouse_conn, queue_scrape, rdap_unsupported_tlds
from time import sleep

parser = argparse.ArgumentParser(description='Queue scrapes')
parser.add_argument('--scrapers', required=False, default='dns', help='Which scraper to run, one or more comma separated (default: dns)')
parser.add_argument('--domain', required=False, help='Single domain')
parser.add_argument('--domain-like', required=False, help='Domain is like')
parser.add_argument('--domain-not-like', required=False, action='append', help='Domain is NOT like')
parser.add_argument('--tld', required=False, help='Limit to this TLD')
parser.add_argument('--ns-domain', required=False, help='Match nameserver domain')
parser.add_argument('--modified-by', required=False, help='Limit to domains last modified by specific scraper')
parser.add_argument('--not-modified-by', required=False, help='Limit to domains NOT last modified by specific scraper')
parser.add_argument('--not-dns-scraped', required=False, action='store_true', help='Limit to domains where dns_scraped is null')
parser.add_argument('--not-rdap-scraped', required=False, action='store_true', help='Limit to domains where rdap_scraped is null')
parser.add_argument('--not-whois-scraped', required=False, action='store_true', help='Limit to domains where whois_scraped is null')
parser.add_argument('--dns-scraped-days-ago', required=False, type=int, help='Limit to domains where dns_scraped is more than x days ago')
parser.add_argument('--rdap-scraped-days-ago', required=False, type=int, help='Limit to domains where dns_scraped is more than x days ago')
parser.add_argument('--whois-scraped-days-ago', required=False, type=int, help='Limit to domains where whois_scraped is more than x days ago')
parser.add_argument('--rdap-supported', required=False, action='store_true', help='Limit to TLDs that implement RDAP')
parser.add_argument('--rdap-unsupported', required=False, action='store_true', help='Limit to TLDs that do not implement RDAP')
parser.add_argument('--not-punycode', required=False, action='store_true', help='Exclude punycode (xn--) domains')
parser.add_argument('--pm-username', required=False, help='Limit to this pm username')
parser.add_argument('--has-pm-username', required=False, action='store_true', help='Limit to rows with pm username')
parser.add_argument('--limit', required=False, type=int, help='Limit number of domains')
parser.add_argument('--shuffle', required=False, action='store_true', help='Shuffle domains to avoid scraping in alphabetical order')
parser.add_argument('--debug', required=False, action='store_true', help='Verbose output for debugging')
parser.add_argument('--noop', required=False, action='store_true', help='No-op. Run the query but don\'t queue')
args = parser.parse_args()

batch_limit = 100_000
scrapers = args.scrapers.split(',')

filters = []
if args.modified_by:
	filters.append(f"modified_by='{args.modified_by}'")
if args.not_modified_by:
	filters.append(f"modified_by!='{args.not_modified_by}'")
if args.domain:
    filters.append(f"domain='{args.domain}'")
    args.limit = 1
if args.domain_like:
    filters.append(f"domain like '{args.domain_like}'")
if args.domain_not_like:
    for not_like in args.domain_not_like:
        filters.append(f"domain not like '{not_like}'")
if args.tld is not None: # can be empty string
	filters.append(f"tld='{args.tld}'")
if args.pm_username:
	filters.append(f"pm_username='{args.pm_username}'")
if args.has_pm_username:
	filters.append(f"pm_username is not null")
if args.not_dns_scraped:
	filters.append("dns_scraped is null")
if args.not_rdap_scraped:
	filters.append("rdap_scraped is null")
if args.not_whois_scraped:
	filters.append("whois_scraped is null")
if args.dns_scraped_days_ago:
    filters.append(f"dns_scraped < date_sub(now(), interval {args.dns_scraped_days_ago} day)")
if args.rdap_scraped_days_ago:
    filters.append(f"rdap_scraped < date_sub(now(), interval {args.rdap_scraped_days_ago} day)")
if args.whois_scraped_days_ago:
    filters.append(f"whois_scraped < date_sub(now(), interval {args.whois_scraped_days_ago} day)")
if args.not_punycode:
    filters.append("domain not like '%xn--%'")
if args.ns_domain:
    filters.append(f"has(ns_domains,'{args.ns_domain}')")
if args.rdap_unsupported:
    sql = ','.join((f"'{tld}'" for tld in rdap_unsupported_tlds()))
    filters.append(f"tld in ({sql})")
if args.rdap_supported:
    sql = ','.join((f"'{tld}'" for tld in rdap_unsupported_tlds()))
    filters.append(f"tld not in ({sql})")
if not filters:
    filters = [ '1' ]
filter_sql = ' and '.join([ f for f in filters ])

order_by = "cityHash64(domain)" if args.shuffle else "domain"

# avoid fetching more rows than we need
if args.limit and args.limit < batch_limit:
    batch_limit = args.limit + 1

last_domain = None
count = 0
conn = clickhouse_conn()
while True:
    # pagination to avoid OOM on clickhouse server. table is ordered by domain so this is efficient
    if last_domain:
        if args.shuffle:
            skip_sql = f"and cityHash64(domain) > cityHash64('{last_domain}')"
        else:
            skip_sql = f"and domain > '{last_domain}'" if last_domain else ''
    else:
        skip_sql = ''
    sql = f"select domain from domains final where {filter_sql} {skip_sql} order by {order_by} limit {batch_limit}"
    print(sql)
    result = conn.execute_iter(sql, settings={'do_not_merge_across_partitions_select_final':1})
    got_rows = False
    for row in result:
        got_rows=True
        count += 1
        if count % 100_000 == 0:
            print("%d domains queued" % count)
        domain = row[0]
        if args.debug:
            print(domain)
        for scraper in scrapers:
            if not args.noop:
                queue_scrape(domain, scraper)
        if args.limit is not None and count > args.limit:
            print("Reached limit of %d" % args.limit)
            got_rows = False # to break out of outer loop
            break
    if not got_rows:
        print("No more rows, exiting")
        break
    last_domain = domain
