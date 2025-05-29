from utils import clickhouse_conn
from sys import stdout
import csv

domains = []
missing = []
results_by_domain = {}
step = 1000
cols='domain,ns,ns_domains,expiration_date,registrar_id'

conn = clickhouse_conn()

with open('../jeffbaron.csv') as f:
    for line in f:
        domain = line.rstrip().lower()
        domains.append(domain)
domains = sorted(domains)

for i in range(0, len(domains), step):
    chunk = domains[i:i+step]
    result = conn.execute("select %s from domains where domain in (%s)" % (cols, ','.join([ "'%s'" % d for d in chunk]))) # XXX sqli
    results_by_domain.update({ r[0]:list(r) for r in result })
    #print(chunk,results_by_domain)
    missing.extend([ d for d in chunk if d not in results_by_domain.keys() ])
    #exit()
    #break

#print(len(missing))
#exit()

csvwriter = csv.writer(stdout)

print(cols)
for domain, row in results_by_domain.items():
    row[1] = ','.join(row[1])
    row[2] = ','.join(row[2])
    if 'trellian.com' in row[1]:
        # workaround
        row[1] = ''
        row[2] = ''
    csvwriter.writerow(row)
