# Agent for Cadia
# This receives scrape requests from kafka queues, runs the requested scraper plugin, and writes to the results queue.
# This is the main script. The scraper logic is in scraper/*.py
import argparse
import concurrent.futures
import sys
import threading
import os.path
from attr import attrs, attrib, validators
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
        self.paused = Gauge('cadia_agent_topic_paused', 'PAUSE status for topic', ['topic'], registry=self.registry)
        self.workers = Gauge('cadia_agent_workers', 'Number of worker threads', ['scraper'], registry=self.registry)
        self.queued = Gauge('cadia_agent_queued', 'Number of requests queued', ['topic'], registry=self.registry)
        self.backoff = Gauge('cadia_agent_backoff', 'Backoff timestamp per topic', ['topic'], registry=self.registry)
        self.scrape_latency = Histogram('cadia_agent_scrape_latency_seconds', 'Scrape latency', ['scraper'], registry=self.registry)
        self.last_send = None
    def loop(self):
        while True:
            self.send()
            sleep(15)
    def send(self):
        for scraper in scrapers.keys():
            self.workers.labels(scraper=scraper).set(len([ 1 for f in future_to_domain.values() if f[1] == scraper ]))
        for k,v in batch.items():
            self.queued.labels(topic=k).set(len(v))
        for k,v in backoff.items():
            self.backoff.labels(topic=k).set(v)
        pushadd_to_gateway('prom-san.trellian.com:9091', job='cadia_agent', grouping_key={'agent':self.agent}, registry=self.registry)
        print("Sent metrics")

@attrs
class Proxy():
    name: str = attrib(validator=validators.instance_of(str))
    host: str = attrib(validator=validators.instance_of(str))
    port: str = attrib(validator=validators.instance_of(int), converter=int)
    username: str = attrib(validator=validators.instance_of(str))
    password: str = attrib(validator=validators.instance_of(str))

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


future_to_domain = {} # future => (domain,scraper,topic) # TODO maybe make a type for that instead. could include other metadata eg retries
max_workers = { 'dns' : 1000, 'rdap' : 80, 'whois' : 4 }
max_poll_records = 100
#backoff = { s:0 for s in scrapers.keys() } # scraper => timestamp
backoff = defaultdict(int)
start_time = time()
done = defaultdict(int)
ratelimiters = defaultdict(RateLimiter) # TODO make own factory method that makes suitable rate limits
ratelimiters['scrape.dns'] = RateLimiter(1000) # higher rate limit for dns # TODO create these from config
ratelimiters['scrape.rdap.rdap_verisign_com_com_v1'] = RateLimiter(100)
ratelimiters['scrape.rdap.rdap_verisign_com_net_v1'] = RateLimiter(100)
ratelimiters['scrape.rdap.rdap_donuts_co_rdap'] = RateLimiter(5)
ratelimiters['scrape.rdap.rdap_publicinterestregistry_org_rdap'] = RateLimiter(5)
ratelimiters['scrape.rdap.rdap_zdnsgtld_com_top'] = RateLimiter(1)
ratelimiters['scrape.rdap.rdap_centralnic_com_xyz'] = RateLimiter(1)
ratelimiters['scrape.whois'] = RateLimiter(2)
batch = {} # topic => [ consumerrecord, ... ]


parser = argparse.ArgumentParser(description='Import zone file')
parser.add_argument('--scrapers', required=False, type=lambda s: s.split(','), help='Which scrapers to run. Comma separated')
parser.add_argument('--id', required=False, default='null', help='Agent id. Used for metrics')
parser.add_argument('--proxies', required=False, help='Proxies to use. Name of group from data/proxies.csv')
args = parser.parse_args()

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
consumer = kafka_consumer_c('agent2')
if args.scrapers:
    pattern = '^scrape.(%s).*' % '|'.join(args.scrapers)
    print("Subscribe to %s" % pattern)
    consumer.subscribe([pattern])
else:
    consumer.subscribe(['^scrape..*'])
print("Connected kafka consumer")
result_producer = kafka_result_producer()

def can_start(topic):
    global future_to_domain, max_workers, ratelimiters
    scraper = topic.split('.')[1]
    # check thread limit per scraper
    if len([f for f in future_to_domain.keys() if future_to_domain[f][1]==scraper]) > max_workers[scraper]:
        #print("can_start(%s): false because at max_workers for %s" % (topic,scraper))
        return False
    if topic in backoff and backoff[topic] > time():
        #print("can_start(%s): false because topic %s is in backoff" % (topic,topic))
        return False
    # check rate limiter (without incrementing it)
    if ratelimiters[topic].over_limit():
        #print("can_start(%s): false because over rate limit for topic %s" % (topic,topic))
        return False
    else:
        ratelimiters[topic].inc()
    return True

def all_throttled(batch):
    for topic in batch.keys():
        if topic in backoff and backoff[topic] > time():
            #print("Topic %s is in backoff" % topic)
            pass
        elif ratelimiters[topic].over_limit():
            print("Scraper %s is at rate limit" % topic)
        else:
            return False
    print("all_throttled true!")
    return True

def need_more_work(batch):
    return True # testing whether we should just always accept work. usually more available in other topics and it sits blocked.

    if not batch:
        return True
    if sum([len(r) for r in batch.values()]) < 2000: # TODO how to set this appropriately? could be based on throughput rates?
        return True
    if all_throttled(batch): # and sum([len(r) for r in batch.values()]) < 2000:
        # Maybe there's work in other topics
        return True
    if sum([1 for topic,r in batch.items() if len(r)>0]) < 10:
        # Make sure we're not stuck on a few blocked scrapers
        return True
    return False

pools = { k: concurrent.futures.ThreadPoolExecutor(max_workers=v) for k,v in max_workers.items() }
while True:
    # Fetch some work
    print("Work queued: %s" % { k:len(v) for k,v in batch.items() if v})
    if need_more_work(batch):
        #print("Getting batch...")
        #print(consumer.paused())
        records = consumer.consume(max_poll_records, 1)
        if records:
            #for tp,rec in records.items():
            parts = set()
            for rec in records:
                if rec.error():
                    raise rec.error()
                topic = rec.topic()
                domain = rec.key().decode('utf-8')
                #print("New request: %s" % domain)
                if topic not in batch:
                    batch[topic] = []
                batch[topic].append(domain)
                parts.add( (topic,rec.partition()) )
            # pause this part so we consume records from different topics and don't get stuck scraping one target
            print("PAUSE %s" % parts)
            consumer.pause([TopicPartition(t,p) for t,p in parts])
            metrics.paused.labels(topic=topic).set(1)
            #batch = { tp.topic : rec for tp,rec in records.items() }
            #print("Got batch:")
            #print(batch)
        #else:
        #    #print("No work found")
    else:
        sleep(0.05) # avoid thrashing

    #print(ratelimiters)
    #print("DONE: %s" % done)
    # Start work from batch if we have capacity
    for topic in batch.keys():
        #print("we have work for topic %s..." % topic)
        scraper = topic.split('.')[1]
        while len(batch[topic]) and can_start(topic):
            #print("We can start work for topic %s" % topic)
            domain = batch[topic].pop(0)

            #print("Start %s %s" % (domain, topic))
            proxy = next_proxy()
            future = pools[scraper].submit(scrape, domain, scraper, proxy)
            future_to_domain[future] = (domain,scraper,topic,proxy)

            # if we've finished all items for that topic, resume so we can consume more
            if len(batch[topic]) == 0:
                # TODO that len=0 limits the speed because the queue has to run empty. but what number would work? could base on rate limit eg 5 sec of work?
                print("RESUME %s" % topic)
                parts = [tp for tp in consumer.assignment() if tp.topic==topic]
                print(parts)
                consumer.resume(parts)
                metrics.paused.labels(topic=topic).set(0)

        #if len(batch[topic]) == 0:
        #    del(batch[topic])

    # check for completed work
    try:
        for future in concurrent.futures.as_completed(future_to_domain.keys(), timeout=0.05):
            domain,scraper,topic,proxy = future_to_domain[future]
            done[topic] += 1
            #print("Got result for %s..."%domain)

            # scrape_domain must return a result dict or throw an exception
            try:
                result = future.result()
                #print("Result for domain %s: %s" % (domain,result))

                result.modified_by = scraper
                result_producer.send("results", key=domain, value=result)
                metrics.scrapes.labels(scraper=scraper, topic=topic, proxy=proxy.name if proxy else '').inc()
                #result_producer.flush()
            except ScraperError as e:
                print("Exception while processing %s: %s" % (domain,e))
                err, error_type, tb = e.args
                if error_type == 'ThrottledException':
                    # TODO detect other temp fails eg Exception while processing 00217.shop: HTTPSConnectionPool(host='rdap.gmoregistry.net', port=443): Max retries exceeded with url: /rdap/domain/00217.shop (Caused by NewConnectionError('<urllib3.connection.VerifiedHTTPSConnection object at 0x7f7bb8092a90>: Failed to establish a new connection: [Errno 101] Network is unreachable'))
                    # TODO pause this topic
                    # TODO re-queue this domain
                    # TODO will need to detect if all queued work is for this topic and somehow avoid it blocking all work
                    print("BACKING OFF %s" % topic)
                    batch[topic].append(domain)
                    backoff[topic] = time() + 10
                else:
                    report_error(domain, err, error_type, tb, scraper, topic)
                    metrics.errors.labels(scraper=scraper, topic=topic, proxy=proxy.name if proxy else '').inc()

            del(future_to_domain[future])
    except concurrent.futures.TimeoutError:
        # Timeout just means as_completed reached 100ms and there's no more completed work
        pass
