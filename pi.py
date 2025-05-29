#!/usr/bin/python3
# Imports PI stats to cadia
from utils import clickhouse_conn, kafka_result_producer, normalize
from result import PIResult


# Fetch from HDFS using clickhouse
# Not writing directly to table because we want to go via kafka/update.py for consistency
ch = clickhouse_conn()
res = ch.execute_iter("select domain,visits,lastdate,hits,geo,visits_arr,rank from hdfs('hdfs://namenode01-san:8020/pi-stats/1year/*.parquet','Parquet')",settings={'max_block_size': 10000})
producer = kafka_result_producer()
count = 0
for row in res:
    normalize_domain = normalize(row[0])
    if normalize_domain == row[0]: # only process domain that is equal to normalize result
        result = PIResult(domain=row[0], visits_12m=row[1], last_visit_date=row[2],hits_12m=row[3], hits_geo=row[4], visits_arr_12m=row[5], pi_rank=row[6])
        producer.send('results', key=row[0], value=result)
        count += 1
        if count % 1000 == 0:
            print("Done %d rows" % count)
    else:
        continue # if invalid proceed to the other domain

producer.flush()
