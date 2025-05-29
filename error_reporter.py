from utils import kafka_consumer, clickhouse_conn
from datetime import datetime
import uuid

ch = clickhouse_conn()
consumer = kafka_consumer('errors','error_reporter')

for msg in consumer:
    domain = msg.key
    row = msg.value

    #print("Domain %s error %s" % (domain, row))

    ch.execute("insert into errors (uuid, domain, error, error_type, file, line, traceback, scraper, topic, timestamp) VALUES",
        [{
             'uuid':uuid.uuid4(),
             'domain':domain,
             'error':row['error'],
             'error_type': row['error_type'],
             'file': row['file'],
             'line': row['line'],
             'traceback': row['traceback'],
             'scraper':row['scraper'],
             'topic': row['topic'],
             'timestamp': datetime.fromisoformat(row['timestamp']),
        }], types_check=True
    )
