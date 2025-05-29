from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from utils import kafka_producer, kafka_consumer, kafka_consumer_c
from random import randint
from time import sleep

#def test_kafka():
#    topic = 'test_kafka_%s' % randint(0, 1_000_000_000)
#    msg = 'msg_%s' % randint(0, 1_000_000_000)
#    key = 'key_%s' % randint(0, 1_000_000_000)
#    #print('connect')
#    cons = kafka_consumer(topic)
#    records = cons.poll(timeout_ms=100) # ensure consumer is connected before producer
#
#    prod = kafka_producer()
#    #print('write...')
#    prod.send(topic, key=key, value=msg)
#    #print('flush...')
#    prod.flush()
#    #print('read...')
#    records = cons.poll(timeout_ms=10000)
#
#    assert records
#    rec = list(records.values())[0][0]
#    assert rec.key == key
#    assert rec.value == msg

#def test_kafka_pause_resume():
#    # Reproducing bug where message loss occurs when using pause/resume
#    record_count = 1000
#    topic = 'test_kafka_%s' % randint(0, 1_000_000_000)
#    cons = kafka_consumer(topic, max_poll_records=100)
#    records = cons.poll(timeout_ms=1000) # ensure consumer is connected before producer
#
#    prod = kafka_producer()
#    for _ in range(1000):
#        prod.send(topic, key='key', value='value')
#    prod.flush()
#
#    received = 0
#
#    # Read first batch
#    records = cons.poll(timeout_ms=1000)
#    tp = list(records.keys())[0]
#    print(tp)
#    msgs = list(records.values())[0]
#    received += len(msgs)
#    print(f"Received first batch: {received}")
#    position = cons.position(tp)
#    print(f"Position: {position}")
#
#    print("pause...")
#    cons.pause(tp)
#    #records = cons.poll(timeout_ms=1000)
#    #assert not records
#    sleep(1)
#    position = cons.position(tp)
#    print(f"Position before resume: {position}")
#    print("resume...")
#    cons.resume(tp)
#    sleep(1)
#    position = cons.position(tp)
#    print(f"Position after resume: {position}")
#
#    print(f"start loop...")
#
#    while received < record_count:
#        records = cons.poll(timeout_ms=10_000)
#        position = cons.position(tp)
#        print(f"Received so far: {received}")
#        print(f"Position: {position}")
#        if not records:
#            continue
#        #    break
#        tp = list(records.keys())[0]
#        msgs = list(records.values())[0]
#        received += len(msgs)
#
#    assert received == record_count

#def test_kafka_pause_resume():
#    # Reproducing bug where message loss occurs when using pause/resume
#    bootstrap_servers = 'kafka01-mel.test:9092,kafka02-mel.test:9092,kafka03-mel.test:9092'
#    record_count = 300
#    max_records_per_poll = 50
#    topic = 'test_kafka_%s' % randint(0, 1_000_000_000)
#
#    # create topic
#    admin = AdminClient({ 'bootstrap.servers' : bootstrap_servers })
#    futures = admin.create_topics([NewTopic(topic, 1)])
#    for t, f in futures.items():
#        f.result(10) # wait for completion
#    print(f"Topic {topic} created")
#    sleep(1) # needs time to replicate?
#
#    # connect
#    # auto.offset.reset=beginning is needed so consumer will seek to beginning. default is latest. hopefully only relevant for this test
#    #cons = Consumer({ 'bootstrap.servers' : bootstrap_servers, 'group.id' : 'pytest', 'auto.offset.reset' : 'beginning' })
#    cons = kafka_consumer_c('pytest', { 'auto.offset.reset' : 'beginning' })
#    cons.subscribe([topic])
#    print("subscribed")
#    sleep(1) # needs time to replicate?
#
#    prod = Producer({ 'bootstrap.servers' : bootstrap_servers })
#    prod.produce(topic, key='key', value='value') # send one record to create topic
#    prod.flush()
#    print("produced one message")
#
#    print("consuming...")
#    records = cons.consume(1, 30) # Make sure we're connected and receiving
#    assert records
#    msg = records[0]
#    assert not msg.error()
#
#    print(f"producer and consumer are working. topic {topic}")
#    print(records[0].value())
#
#    for i in range(record_count):
#        prod.produce(topic, key=f'key_{i}', value=f'value_{i}')
#    prod.flush()
#
#    print("sent and flushed")
#
#    received = 0
#
#    # Read first batch
#    records = cons.consume(max_records_per_poll, 1)
#    print(records)
#    assert records
#    for msg in records:
#        assert not msg.error()
#        print(msg.value())
#    received += len(records)
#
#    #tp = list(records.keys())[0]
#    #print(tp)
#    #msgs = list(records.values())[0]
#    #received += len(msgs)
#    print(f"Received first batch: {received}")
#    #position = cons.position(tp)
#    position = cons.position(cons.assignment())[0].offset
#    print(f"Position: {position}")
#    assert position == received + 1
#
#    print("pause...")
#    tp = cons.assignment()[0]
#    cons.pause([tp])
#    # Data loss happened during this poll
#    records = cons.poll(1)
#    assert not records # Should get timeout
#    position = cons.position([tp])[0].offset
#    print(f"Position before resume: {position}")
#    print("resume...")
#    cons.resume([tp])
#    sleep(1)
#    position = cons.position([tp])[0].offset
#    print(f"Position after resume: {position}")
#    assert position == received + 1
#
#    print(f"start loop...")
#
#    while received < record_count:
#        records = cons.consume(max_records_per_poll, 1)
#        print(records)
#        assert records
#        for msg in records:
#            assert not msg.error()
#            print(msg.value())
#        received += len(records)
#        position = cons.position(cons.assignment())[0].offset
#        assert position == received + 1
#
#    assert received == record_count
