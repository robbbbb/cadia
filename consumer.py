#!/usr/bin/python3
# Dummy consumer to show results for debugging

from utils import kafka_consumer

consumer = kafka_consumer('results','test-group')
for msg in consumer:
     print (msg)
