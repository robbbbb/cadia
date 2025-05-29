from utils import postgres_conn, clickhouse_conn
import psycopg2.errors

ch = clickhouse_conn()
pg = postgres_conn()

cur = pg.cursor()
results = ch.execute("select id, criteria, timestamp from searches")

for row in results:
    cur.execute("insert into searches (id, criteria, timestamp) values (%s,%s,%s) on conflict do nothing", row)

pg.commit()
