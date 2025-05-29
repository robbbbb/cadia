from utils import clickhouse_conn, kafka_producer, queue_scrape, tld
from time import sleep

producer = kafka_producer()

conn = clickhouse_conn()
#print("Fetching domains limit %d offset %d" % (limit,offset))
#result = conn.execute("select domain from domains group by domain having argMax(dns_exists, last_modified) is null limit 10")
#result = conn.execute("select distinct(domain) from domains final where length(ns)>0 and length(ns_domains)==0 and modified_by='dns'") # re-run to add ns_domains
#result = conn.execute("select distinct(domain) from domains final where has(ns_domains, 'abovedomains.com') OR has(ns_domains, 'above.com') order by domain")
#result = conn.execute("select distinct(domain) from domains final where has(ns, 'ns1.trellian.com') order by domain")

#result = conn.execute_iter("select domain from domains final where modified_by='zonefile' and tld='com'",settings={'max_block_size': 10000})
#result = conn.execute_iter("select distinct(domain) from domains final where has(ns, 'ns1.trellian.com')")
#result = conn.execute_iter("select distinct(domain) from domains final where tld='com' and registered is null")
result = conn.execute_iter("select distinct(domain) from domains final where modified_by='pi' or modified_by='zonefile'")

count = 0
for row in result:
    count += 1
    if count % 1000 == 0:
        print("%d domains queued" % count)
    domain = row[0]
    #print(domain)

    queue_scrape(domain, 'dns', producer)

    if tld(domain) in ['gov','us']:
        queue_scrape(domain, 'whois', producer)
    else:
        queue_scrape(domain, 'rdap', producer)

producer.flush()
