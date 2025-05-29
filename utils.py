# Common utilities
from clickhouse_driver import Client
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Consumer, Producer
from datetime import date, datetime
from functools import lru_cache
from os import getenv
from time import time, sleep
import attr
import json
import memcache
import traceback
import dns.resolver
import psycopg2

# Considering using msgpack instead of json:
#import msgpack
#serializer = msgpack.packb
#deserializer = lambda v: msgpack.unpackb(v, raw=False)

bootstrap_servers = 'kafka04-san.trellian.com:9092,kafka05-san.trellian.com:9092,kafka06-san.trellian.com:9092' # TODO config
#bootstrap_servers = 'kafka01-mel.test:9092,kafka02-mel.test:9092,kafka03-mel.test:9092' # XXX DO NOT COMMIT XXX

# Columns in DB
array_cols = [ 'ns', 'ns_domains', 'txt', 'mx', 'addr', 'geo', 'asn', 'status' ]
bool_cols = [ 'registered' ]
all_cols = [ 'domain', 'tld', 'registrar_id', 'registration_date', 'expiration_date', 'transfer_date', 'last_changed_date', 'hits_12m', 'visits_12m', 'visits_arr_12m', 'hits_geo', 'pi_rank', 'last_modified', 'modified_by', 'dns_scraped', 'pm_username', 'pm_added', 'pm_blacklisted',
            'registrant_name', 'registrant_phone', 'registrant_email', 'registrant_organization', 'registrant_stateprovince', 'registrant_country',
            'admin_name', 'admin_phone', 'admin_email', 'admin_organization', 'admin_stateprovince', 'admin_country',
            'tech_name', 'tech_phone', 'tech_email', 'tech_organization', 'tech_stateprovince', 'tech_country'
            ] + array_cols + bool_cols

MAX_WAIT_TIME = 600
WAIT_INTERVAL = 5

def kafka_bootstrap_servers():
    return bootstrap_servers

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def kafka_producer(**kwargs):
    key_serializer = lambda k: k.encode('utf-8').lower()
    value_serializer = lambda v: json.dumps(v, default=json_serial).encode('utf-8')
    return KafkaProducer(bootstrap_servers=bootstrap_servers, key_serializer=key_serializer, value_serializer=value_serializer, **kwargs)

def kafka_result_producer(**kwargs):
    """Kafka producer that serialises Result objects"""
    key_serializer = lambda k: k.encode('utf-8').lower()
    value_serializer = lambda v: v.serialize().encode('utf-8')
    return KafkaProducer(bootstrap_servers=bootstrap_servers, key_serializer=key_serializer, value_serializer=value_serializer, **kwargs)

def kafka_consumer(topics,group_id=None,**kwargs):
    key_deserializer = lambda k: k.decode('utf-8')
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    return KafkaConsumer(topics, group_id=group_id, bootstrap_servers=bootstrap_servers, key_deserializer=key_deserializer, value_deserializer=value_deserializer, **kwargs)

def kafka_consumer_c(group_id, args={}):
    conf = { 'bootstrap.servers' : bootstrap_servers, 'group.id' : group_id }
    conf.update(args)
    return Consumer(conf)

def clickhouse_conn():
    password = getenv('CADIA_UPDATE_DB_PASS')
    if password is None:
        raise ValueError("Can't read cadia db password from env CADIA_UPDATE_DB_PASS")
    return TimedClickhouseClient(host='cadia-ch',port=9000,database='cadia',user='cadia_update',password=getenv('CADIA_UPDATE_DB_PASS'))

def postgres_conn():
    password = getenv('CADIA_UPDATE_DB_PASS')
    if password is None:
        raise ValueError("Can't read cadia db password from env CADIA_UPDATE_DB_PASS")
    return psycopg2.connect(f"dbname=cadia host=cadia-db user=cadia_update password={password}")

_mc_client = None
def memcached_connect():
    global _mc_client
    if _mc_client is None:
        res = dns.resolver.Resolver()
        answer = res.resolve('parking-san-mc.trellian.com','A')
        ips = [ r.address for r in answer if r.rdtype==dns.rdatatype.A ]
        _mc_client = memcache.Client(["%s:11211" % ip for ip in ips], debug=0)
    return _mc_client

_msg_count = 0
def round_robin_partitioner(key, all_partitions, available):
    # used as partitioner= in producer below to distribute messages evenly. Default mumur hash doesn't do well
    global _msg_count
    _msg_count += 1
    return all_partitions[_msg_count % len(all_partitions)]

scrape_producer = None
# Uses global producer object by default. Option to pass own to control config and flushing
def queue_scrape(domain,scraper,producer=None):
    global scrape_producer
    if producer is None:
        if scrape_producer is None:
            # Initialise the global and assign it
            # RoundRobinPartitioner because default hash gives uneven distribution
            scrape_producer = kafka_producer(partitioner=round_robin_partitioner)
        producer = scrape_producer

    # whois and rdap provide the same data through different protocols so we treat them as equivalent
    # and refer to it as "whois" in the UI for simplicity. This decides which to actually use. Prefers RDAP
    if scraper == 'whois' and tld(domain) not in rdap_unsupported_tlds():
        scraper = 'rdap'

    if scraper == 'rdap':
        #from scraper.rdap import RDAPScraper
        #shard = RDAPScraper().shard_for_domain(domain)
        #if shard is None:
        #    # RDAP not supported for this TLD
        #    return
        producer.send('scrape.rdap', key=domain, value={}) # did have .+shard, removed for testing single topic
    elif scraper == 'dns':
        producer.send('scrape.dns', key=domain, value={})
    elif scraper == 'whois':
        producer.send('scrape.whois', key=domain, value={})
    else:
        raise ValueError()
    producer.flush()

error_producer = None
def report_error(domain, err, error_type, tb, scraper, topic):
    global error_producer
    if error_producer is None:
        error_producer = kafka_producer()
    
    file_name, line_number, _, _ = traceback.extract_tb(tb)[-1]
    tb_str = "\n".join(traceback.format_tb(tb))
    
    error_info = {
        'domain': domain,
        'error': err,
        'error_type': error_type,
        'file': file_name,
        'line': line_number,
        'traceback' : tb_str,
        'scraper': scraper,
        'topic': topic,
        'timestamp': datetime.now().isoformat()
    }
    error_producer.send('errors', key=domain, value=error_info),
    error_producer.flush()

tlds = None
def all_tlds():
    # load tlds from file on first call
    global tlds
    if tlds is None:
        with open('/usr/share/publicsuffix/public_suffix_list.dat', encoding='utf-8') as f:
            lines = [ line.rstrip() for line in f ]
            tlds = [ l for l in lines if l and not l.startswith('//') ]
    return tlds

@lru_cache(maxsize=5000)
def tld(domain):
    tlds = all_tlds()

    # longest matching tld
    try:
        return max([ t for t in tlds if domain.endswith('.'+t) ], key=len)
    except ValueError:
        return None

@lru_cache(maxsize=5000)
def normalize(domain):
    parts = domain.split('.')
    t = tld(domain)
    if t is None:
        return None
    tld_parts = t.count('.') + 1 # eg com = 1, com.au = 2
    return '.'.join(parts[-(tld_parts+1):])

@lru_cache(maxsize=1)
def rdap_unsupported_tlds():
    tlds = all_tlds()

    result = ['au', 'be', 'cm', 'cn', 'co', 'co.ke', 'co.kr', 'co.za', 'com.vn', 'de', 'eu', 'gr', 'hu', 'in', 'io', 'it', 'ru', 'us']
    result.extend([ t for t in tlds if t.endswith('.au')])

    return set(result)


class RateLimiter:
    # Sliding window rate limiter based on https://dev.to/satrobit/rate-limiting-using-the-sliding-window-algorithm-5fjn

    def __init__(self, capacity=8, time_unit=1):
        self.capacity = capacity
        self.time_unit = time_unit

        self.cur_time = time()
        self.pre_count = capacity
        self.cur_count = 0

    def __repr__(self):
        return "RateLimit(%0.1f/%d)" % (self.ec(),self.capacity)


    def ec(self):
        return max(0,(self.pre_count * (self.time_unit - (time() - self.cur_time)) / self.time_unit) + self.cur_count)

    def inc(self, inc=1):
        if (time() - self.cur_time) > self.time_unit:
            self.cur_time = time()
            self.pre_count = self.cur_count
            self.cur_count = 0

        #print("cur_count %d pre_count %d ec %0.2f" % (self.cur_count, self.pre_count, ec))

        if (self.ec() > self.capacity):
            return False

        self.cur_count += inc 
        return True

    def over_limit(self):
        # Check if over limit without incrementing
        return not self.inc(0)

def print_timing(func):
    def wrapper(obj, *args, **kwargs):
        start = time()
        result = func(obj, *args, **kwargs)
        duration = time() - start
        print(f'{func.__name__} {duration:.4f} {args}')
        return result
    return wrapper

class TimedClickhouseClient(Client):
    @print_timing
    def execute(self, *args, **kwargs):
        result = super().execute(*args, **kwargs)
        return result
    @print_timing
    def execute_iter(self, *args, **kwargs):
        result = super().execute_iter(*args, **kwargs)
        return result

_cols_and_types = None
def db_cols():
    global _cols_and_types
    if _cols_and_types is None:
        ch = clickhouse_conn()

        result = ch.execute("select name,type from system.columns  where database='cadia' and table='domains'")
        _cols_and_types = { str(r[0]): str(r[1]) for r in result }
    return _cols_and_types

def db_default_row():
    cols = db_cols()

    row = { col:'' for col in cols.keys() }
    # nullable columns default to None
    row.update({ col:None for col,typ in cols.items() if typ.startswith('Nullable') })
    # array columns default to empty list
    row.update({ col:[] for col,typ in cols.items() if typ.startswith('Array') })

    return row


def check_mutations():
    conn = clickhouse_conn()
    print("Waiting for mutation to complete...")
    start_time = time()
    while time() - start_time < MAX_WAIT_TIME:
        mutation_in_progress = conn.execute("""
            SELECT 1 FROM system.mutations
            WHERE NOT is_done AND table = 'domains' AND database = 'cadia'
        """)
        if mutation_in_progress:
            print(f"Mutation in progress... waiting {WAIT_INTERVAL}s")
            sleep(WAIT_INTERVAL)
        else:
            print("Mutation completed.")
            break
    else:
        raise TimeoutError("Mutation did not finish within expected time.")
