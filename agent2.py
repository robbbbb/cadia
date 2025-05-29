# Agent for Cadia
# This receives scrape requests from kafka queues, runs the requested scraper plugin, and writes to the results queue.
# This is the main script. The scraper logic is in scraper/*.py
import argparse
import concurrent.futures
import sys
import threading
import os.path
import gc
from attr import attrs, attrib, validators, define
from confluent_kafka import TopicPartition
from datetime import datetime, timedelta
from utils import kafka_consumer, kafka_result_producer, report_error, RateLimiter, kafka_consumer_c
from collections import defaultdict, namedtuple
from scraper.base import ThrottledException
from scraper.dns import DNSScraper
from scraper.rdap import RDAPScraper
from scraper.whois import WHOISScraper
from time import time, sleep
from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, pushadd_to_gateway

# TODO initialise this depending on config / cli args
scrapers = { 'dns' : DNSScraper(), 'rdap' : RDAPScraper(), 'whois': WHOISScraper() }

class ScraperError(RuntimeError):
    pass

class Metrics():
    def __init__(self, agent):
        self.agent = agent
        self.registry = CollectorRegistry()
        self.scrapes = Counter('cadia_agent_scrapes', 'Number of successful scrapes', ['scraper','topic','proxy'], registry=self.registry)
        self.errors = Counter('cadia_agent_errors', 'Number of failed scrapes', ['scraper','topic','proxy'], registry=self.registry)
        self.workers = Gauge('cadia_agent_workers', 'Number of worker threads', ['scraper'], registry=self.registry)
        self.queued = Gauge('cadia_agent_queued', 'Number of requests queued', ['topic', 'shard'], registry=self.registry)
        self.backoff = Gauge('cadia_agent_backoff', 'Backoff timestamp per topic', ['topic', 'shard'], registry=self.registry)
        self.ratelimiter_rate = Gauge('cadia_agent_ratelimiter_rate', 'Rate limiter current rate', ['topic', 'shard'], registry=self.registry)
        self.ratelimiter_limit = Gauge('cadia_agent_ratelimiter_limit', 'Rate limiter limit', ['topic', 'shard'], registry=self.registry)
        self.scrape_latency = Histogram('cadia_agent_scrape_latency_seconds', 'Scrape latency', ['scraper'], registry=self.registry)
        self.last_send = None
    def loop(self):
        while True:
            self.send()
            sleep(15)
    def send(self):
        for scraper in scrapers.keys():
            self.workers.labels(scraper=scraper).set(len([ 1 for f in future_to_domain.values() if f[1].scraper() == scraper ]))
        for k,v in batch.items():
            self.queued.labels(topic=k.topic, shard=k.shard).set(len(v))
        for k,v in backoff.items():
            self.backoff.labels(topic=k.topic, shard=k.shard).set(v)
        for k,v in ratelimiters.items():
            self.ratelimiter_rate.labels(topic=k.topic, shard=k.shard).set(v.ec())
            self.ratelimiter_limit.labels(topic=k.topic, shard=k.shard).set(v.capacity)

        pushadd_to_gateway('prom-san.trellian.com:9091', job='cadia_agent', grouping_key={'agent':self.agent}, registry=self.registry)
        #print("Sent metrics")

@attrs
class Proxy():
    name: str = attrib(validator=validators.instance_of(str))
    host: str = attrib(validator=validators.instance_of(str))
    port: str = attrib(validator=validators.instance_of(int), converter=int)
    username: str = attrib(validator=validators.instance_of(str))
    password: str = attrib(validator=validators.instance_of(str))

#@attrs
@define(frozen=True)
# Target is a combination of topic and shard. It's the unit for the work queue, rate limiting and throttling
class Target():
    topic: str = attrib(validator=validators.instance_of(str))
    shard: str = attrib(validator=validators.optional(validators.instance_of(str)))

    @classmethod
    def by_topic_domain(cls,topic,domain):
        return Target(topic=topic, shard=scrapers[topic.split('.')[1]].shard_for_domain(domain))

    def scraper(self):
        return self.topic.split('.')[1]

_scrape_counter = 0
def next_proxy():
    global _scrape_counter
    proxy = proxies[_scrape_counter % len(proxies)] if proxies else None
    _scrape_counter += 1
    return proxy


def scrape(domain, scraper, proxy):
    #print("Scrape %s on %s" % (domain,scraper))
    try:
        start = datetime.now()
        result = scrapers[scraper].scrape_domain(domain, proxy)
        # This metric is limited - only counts successful scrapes, and we don't know the topic
        metrics.scrape_latency.labels(scraper=scraper).observe((datetime.now()-start).total_seconds())
        return result
    except Exception as e:
        # Raising the original exception class does not work reliably - it may not be imported here, or may require more args
        # Instead we store the message, type and traceback in a RuntimeError and unpack it when reporting
        exc_type, value, traceback = sys.exc_info()
        raise ScraperError(str(e), exc_type.__name__, traceback)

#consumer = {} # scraper => consumer object
#for s in scrapers.keys():
#    print("Connect consumer for %s..." % s)
#    consumer[s] = kafka_consumer('scrape.'+s, 'agent.'+s, consumer_timeout_ms=100)
#    consumer[s].subscribe(pattern='scrape.'+s+'.*')
#    print("Connected consumer for %s" % s)

def read_proxies(group):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rel_path = "data/proxies.csv"
    abs_file_path = os.path.join(script_dir, rel_path)
    result = []
    with open(abs_file_path) as f:
        for line in f:
            (gr, *other) = line.rstrip().split(',')
            if gr==group:
                result.append(Proxy(*other))
    return result



parser = argparse.ArgumentParser(description='Import zone file')
parser.add_argument('--scrapers', required=False, type=lambda s: s.split(','), help='Which scrapers to run. Comma separated')
parser.add_argument('--id', required=False, default='null', help='Agent id. Used for metrics')
parser.add_argument('--proxies', required=False, help='Proxies to use. Name of group from data/proxies.csv')
args = parser.parse_args()

future_to_domain = {} # future => (domain,scraper,topic) # TODO maybe make a type for that instead. could include other metadata eg retries
max_workers = { 'dns' : 1200, 'rdap' : 80, 'whois' : 6 }
max_poll_records = 1000 if 'dns' in scrapers else 100 # TODO should have separate consumer for each with different limit?
#backoff = { s:0 for s in scrapers.keys() } # scraper => timestamp
backoff = defaultdict(int)
start_time = time()
done = defaultdict(int)
batch = {} # topic => [ consumerrecord, ... ]

ratelimiters = defaultdict(RateLimiter) # TODO make own factory method that makes suitable rate limits
# TODO create these from config
if not args.scrapers or 'dns' in args.scrapers:
    ratelimiters[Target(topic='scrape.dns', shard=None)] = RateLimiter(1000) # higher rate limit for dns
if not args.scrapers or 'rdap' in args.scrapers:
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_verisign_com_com_v1')] = RateLimiter(100)
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_verisign_com_net_v1')] = RateLimiter(100)
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_identitydigital_services_rdap')] = RateLimiter(5)
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_donuts_co_rdap')] = RateLimiter(5)
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_publicinterestregistry_org_rdap')] = RateLimiter(5)
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_zdnsgtld_com_top')] = RateLimiter(1)
    ratelimiters[Target(topic='scrape.rdap', shard='rdap_centralnic_com_xyz')] = RateLimiter(1)
if not args.scrapers or 'whois' in args.scrapers:
    ratelimiters[Target(topic='scrape.whois', shard=None)] = RateLimiter(2)

metrics = Metrics(args.id)
threading.Thread(target=metrics.loop).start() # TODO handle crashes in that thread

if args.proxies:
    proxies = read_proxies(args.proxies)
    if not proxies:
        raise ValueError(f"No proxies found in group {args.proxies}")
    # increase max_workers to scale with the number of proxies
    max_workers = { k: v * len(proxies) for k,v in max_workers.items() }
else:
    proxies = None

print("Connecting kafka consumer...")
consumer = kafka_consumer_c('agent')
if args.scrapers:
    pattern = '^scrape.(%s)$' % '|'.join(args.scrapers)
    print("Subscribe to %s" % pattern)
    consumer.subscribe([pattern])
else:
    consumer.subscribe(['^scrape..*'])
print("Connected kafka consumer")
result_producer = kafka_result_producer()

def can_start(target):
    global future_to_domain, max_workers, ratelimiters
    scraper = target.scraper()
    # check thread limit per scraper
    if len([f for f in future_to_domain.values() if f[1].scraper()==scraper]) > max_workers[scraper]:
        #print("can_start(%s): false because at max_workers for %s" % (topic,scraper))
        return False
    #if topic in backoff and backoff[topic] > time():
    #    #print("can_start(%s): false because topic %s is in backoff" % (topic,topic))
    #    return False
    # check rate limiter (without incrementing it)
    #if ratelimiters[topic].over_limit():
    #    #print("can_start(%s): false because over rate limit for topic %s" % (topic,topic))
    #    return False
    #else:
    #    ratelimiters[topic].inc()
    return True

def all_throttled(batch):
    for target in batch.keys():
        if target in backoff and backoff[target] > time():
            #print("Topic %s is in backoff" % topic)
            pass
        elif ratelimiters[target].over_limit():
            print("Scraper %s is at rate limit" % target)
        else:
            return False
    print("all_throttled true!")
    return True

def need_more_work(batch):
    #return True # testing whether we should just always accept work. usually more available in other topics and it sits blocked.

    if not batch:
        print("need_more_work: true because batch is empty")
        return True

    # Fetch up to 5 items per worker limit
    # Note that this isn't accurate if we're running multiple scrapers in the same agent
    # now that all shards are combined in one queue we don't control which shard domains will be for
    # so best we can do is ensure we have enough items across all shards for that scraper
    scrapers = { target.scraper() for target in batch.keys() }
    for scraper in scrapers:
        queue_len = sum([ len(records) for target,records in batch.items() if target.scraper() == scraper ])
        print(f"need_more_work: queue_len is {queue_len} for scraper {scraper}")
        if queue_len < max_workers[scraper] * 5:
            print(f"need_more_work: true because have {queue_len} records for {scraper}")
            return True

    #if sum([len(r) for r in batch.values()]) < 2000: # TODO how to set this appropriately? could be based on throughput rates?
    #    return True
    #if all_throttled(batch): # and sum([len(r) for r in batch.values()]) < 2000:
    #    # Maybe there's work in other topics
    #    return True
    # Disabled because all rdap now combined into one, most scrapes are for .com so this just resulted in too many records being queued
    #if sum([1 for target,r in batch.items() if len(r)>0]) < 10:
    #    # Make sure we're not stuck on a few blocked scrapers
    #    return True
    print("need_more_work: false, default")
    return False

pools = { k: concurrent.futures.ThreadPoolExecutor(max_workers=v) for k,v in max_workers.items() }
while True:
    # Fetch some work
    #print("Work queued: %s" % { k:len(v) for k,v in batch.items() if v})
    if need_more_work(batch):
        print("Getting batch...")
        records = consumer.consume(max_poll_records, 0)
        if records:
            #for tp,rec in records.items():
            parts = set()
            for rec in records:
                if rec.error():
                    raise rec.error()
                topic = rec.topic()
                domain = rec.key().decode('utf-8')
                #print("New request: %s" % domain)
                target = Target.by_topic_domain(topic,domain)
                if target.scraper() == 'rdap' and target.shard is None:
                    # Temporary until TLDs are fixed https://app.shortcut.com/trillion-1/story/11198/fix-rows-with-incorrect-tlds
                    print(f"skipping domain {domain} because it is not on a rdap-supported TLD")
                    continue
                if target not in batch:
                    batch[target] = []
                batch[target].append(domain)
                print(f"Added domain {domain}")
            #    parts.add( (topic,rec.partition()) )
            #batch = { tp.topic : rec for tp,rec in records.items() }
            #print("Got batch:")
            #print(batch)
        else:
            print("No more records found")
            #break
    print("finished while(need_more_work)")

    #print(ratelimiters)
    #print("DONE: %s" % done)
    # Start work from batch if we have capacity
    for target in batch.keys():
        #print("we have work for topic %s..." % topic)
        while len(batch[target]) and can_start(target):
            #print("We can start work for topic %s" % topic)
            domain = batch[target].pop(0)

            # Check rate limit
            if not ratelimiters[target].inc():
                # at rate limit, can not proceed
                print(f"WARNING: at rate limit for {target}, can not process domain {domain}, pushing it back on queue")
                batch[target].append(domain)
                break
                # TODO metric, retry for some time, requeue

            #print("Start %s %s" % (domain, topic))
            proxy = next_proxy()
            future = pools[target.scraper()].submit(scrape, domain, target.scraper(), proxy)
            future_to_domain[future] = (domain,target,proxy)

    # check for completed work
    try:
        for future in concurrent.futures.as_completed(future_to_domain.keys(), timeout=0.05):
            domain,target,proxy = future_to_domain[future]
            done[target] += 1
            #print("Got result for %s..."%domain)

            # scrape_domain must return a result dict or throw an exception
            try:
                result = future.result()
                #print("Result for domain %s: %s" % (domain,result))

                result.modified_by = target.scraper()
                result_producer.send("results", key=domain, value=result)
                metrics.scrapes.labels(scraper=target.scraper(), topic=target.topic, proxy=proxy.name if proxy else '').inc()
                #result_producer.flush()
            except ScraperError as e:
                print("Exception while processing %s: %s" % (domain,e))
                err, error_type, tb = e.args
                if error_type == 'ThrottledException':
                    # TODO detect other temp fails eg Exception while processing 00217.shop: HTTPSConnectionPool(host='rdap.gmoregistry.net', port=443): Max retries exceeded with url: /rdap/domain/00217.shop (Caused by NewConnectionError('<urllib3.connection.VerifiedHTTPSConnection object at 0x7f7bb8092a90>: Failed to establish a new connection: [Errno 101] Network is unreachable'))
                    # TODO drop from our batch and re-queue in kafka this domain and all others with the same shard
                    print("BACKING OFF %s" % target)
                    #batch[target].append(domain)
                    print(f"FIXME: discarding queued domains for target {target}")
                    batch[target] = []
                    backoff[target] = time() + 10
                else:
                    report_error(domain, err, error_type, tb, target.scraper(), target.topic)
                    metrics.errors.labels(scraper=target.scraper(), topic=target.topic, proxy=proxy.name if proxy else '').inc()

            del(future_to_domain[future])
    except concurrent.futures.TimeoutError:
        # Timeout just means as_completed reached 100ms and there's no more completed work
        pass

    sleep(0.1) # to avoid thrashing
