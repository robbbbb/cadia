from utils import clickhouse_conn, kafka_producer
from scraper.rdap import RDAPScraper
from time import sleep

producer = kafka_producer()
scraper = RDAPScraper()

limit = 100000
offset = 0

#while True:
conn = clickhouse_conn()
#    #print("Fetching domains limit %d offset %d" % (limit,offset))
#    #result = conn.execute("select domain from domains group by domain having argMax(dns_exists, last_modified) is null limit 10")
#    #result = conn.execute("select distinct(domain) from domains final where length(ns)>0 and length(ns_domains)==0 and modified_by='dns'") # re-run to add ns_domains
#    #result = conn.execute("select distinct(domain) from domains final where has(ns_domains, 'abovedomains.com') OR has(ns_domains, 'above.com') order by domain")
#    #result = conn.execute("select distinct(domain) from domains final where has(ns, 'ns1.trellian.com') order by domain")
#

conn = clickhouse_conn()
domains = []
count = 0
#with open('../domains_2023-06-07_similarweb_sites') as f:
#with open('../jeffbaron.csv') as f:

#    for line in f:
#        domain = line.rstrip().lower()
#        result = conn.execute("select rdap_json='' from domains where domain='%s'"%domain)
#        if not result or result[0][0]: # no row or rdap_json is not empty
#            domains.append(domain)
#            print(domain)


while True:
    #result = conn.execute("select distinct(domain) from domains final where tld in ('com','net','org','info') and rdap_json='' order by domain limit %d offset %d" % (limit,offset))
    result = conn.execute("select distinct(domain) from domains final where modified_by='dns' order by domain")
    for row in result:
            #for domain in domains:
            #domain = domain.rstrip().lower()
            count += 1
            if count % 1000 == 0:
                print("%d domains queued" % count)
            domain = row[0]
            #print(domain)
            shard = scraper.shard_for_domain(domain)
            if shard is None:
                continue
            #print(shard)
            producer.send('scrape.rdap.'+shard, key=domain, value={})
    offset += limit
    producer.flush()
    break
#break # now doing all in one batch

