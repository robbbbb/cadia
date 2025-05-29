
#!/usr/bin/python3
# Imports PM ownership details to cadia
import os
from kafka import KafkaProducer
import psycopg2
from utils import kafka_result_producer
from result import AbovePMResult
from utils import normalize, tld


producer: KafkaProducer = kafka_result_producer()
conn = psycopg2.connect("dbname=parking host=parking-db user=parking_website password={}".format(os.environ.get('PARKING_WEBSITE_DB_PASS')))
cur = conn.cursor()

cur.execute("select dmn_name.nam as domain, usr.nam as username, blacklisted, added from dmn_name join usr on (owner=usr.id)")

count = 0
for row in cur:
    normalize_domain = normalize(row[0])
    if normalize_domain == row[0]: # only process domain that is correct
        result = AbovePMResult(domain=row[0], pm_username=row[1], pm_blacklisted=row[2], pm_added=row[3])
        producer.send('results', key=row[0], value=result)
        count += 1
        if count % 1000 == 0:
            print("Done %d rows" % count)
    else:
        continue # skip invalid domains

producer.flush()