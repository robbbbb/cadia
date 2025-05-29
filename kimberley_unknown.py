import pymysql.cursors
from utils import kafka_producer

conn=pymysql.connect(host="netfleet_kimberley-db",user="netfleet_kimberley",password="yo9Fivoo",database="netfleet_kimberley")
cur = conn.cursor()

cur.execute("""SELECT domain FROM domains WHERE whois_check_status = 'Not Available'""")

producer = kafka_producer()
for row in cur:
    print(row)
    producer.send('scrape.dns', key=row[0], value=None )
producer.flush()
