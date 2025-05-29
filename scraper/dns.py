# DNS scraper
from scraper.base import BaseScraper
from result import DNSResult
from utils import tld, normalize
import dns.resolver

class DNSScraper(BaseScraper):
    cached_ns_ips = {}

    def __init__(self):
        self.resolver = dns.resolver.Resolver() #configure=False)
        #self.resolver.nameservers = ["127.0.0.1"]
        self.resolver.timeout = 5
        self.resolver.lifetime = 5

    def scrape_domain(self, domain, proxy):

        # TODO: handling:
        #dns.resolver.NoNameservers
        #dns.resolver.NoAnswer:
        # how to report that this domain had errors? send to retry queue? send a result as well?

        addr = ns = mx = txt = ns_domains = []
        rcode = ''
        # Using lower level API for A query to catch rcode
        try:
            query=dns.message.make_query(domain, 'A')
            response = dns.query.udp(query, self.resolver.nameservers[0], timeout=5) # TODO fallback to TCP?
            rcode = dns.rcode.to_text(response.rcode()) # eg SERVFAIL
            if response.answer:
                answer = response.answer
                rrs = [ r for r in answer if r.rdtype==dns.rdatatype.A ]
                #b = [ a for a in answer if a.rdtype==dns.rdatatype.A ]
                addr = sorted([r.address for rr in rrs for r in rr])
        except (dns.exception.DNSException):
            rcode = 'timeout?' # TODO correct code from exception

        try:
            ns = DNSScraper.query_ns(domain)
            if ns:
                ns_domains = sorted(list({normalize(n) for n in ns if normalize(n) is not None}))  #Extracts the domain and makes unique
        except dns.exception.DNSException:
            pass
            
        try:
            answer = self.resolver.resolve(domain, "MX")
            mx = sorted([rr.exchange.to_text().rstrip('.') for rr in answer])
        except (dns.exception.DNSException):
            pass

        try:
            answer = self.resolver.resolve(domain, "TXT")
            txt = sorted([rr.strings[0].decode('utf-8',errors='replace') for rr in answer])
        except (dns.exception.DNSException):
            #except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
            pass
        return DNSResult(domain=domain, addr=addr, rcode=rcode, ns=ns, mx=mx, txt=txt, ns_domains=ns_domains, dns_exists=any(ns))

    def resolver_for_tld(tld):
        try:
            if tld not in DNSScraper.cached_ns_ips:
                # First find IPs of TLD nameservers
                resp = dns.resolver.resolve(tld+'.','NS', raise_on_no_answer=False)
                ns = [ r.to_text() for r in resp ]
                if not ns:
                    # if no answer, check authority instead. Eg .co.jp returns an SOA referring to .jp
                    # strip the trailing dot
                    other_tld = str(resp.response.authority[0].name).rstrip('.')
                    if other_tld and other_tld != tld:
                        return DNSScraper.resolver_for_tld(other_tld)
                answers = []
                for n in ns:
                    try:
                        answers.append( dns.resolver.resolve(n) )
                    except:
                        pass # Ignore, if some NS are invalid we use others
                ips = list(set([ r.to_text() for a in answers for r in a ]))
                #print(f"IPs for {tld} are: {ips}")
                if not ips:
                    raise ValueError(f"Unable to determine nameservers for tld '{tld}'")
                DNSScraper.cached_ns_ips[tld] = ips

            tld_resolver = dns.resolver.Resolver(configure=False)
            tld_resolver.nameservers = DNSScraper.cached_ns_ips[tld]
            tld_resolver.timeout = 2
            tld_resolver.lifetime = 5
            return tld_resolver
        except Exception as e:
            raise ValueError(f"Unable to determine nameservers for tld '{tld}'") from e


    def query_ns(domain):
        # Checking against TLD DNS servers, end users' servers are inconsistent
        tld_resolver = DNSScraper.resolver_for_tld(tld(domain))

        # Get NS from the AUTHORITY section
        answers = tld_resolver.resolve(domain,'NS',raise_on_no_answer=False)
        if not answers.response.authority:
            return []
        ns = sorted([ r.to_text().rstrip('.') for r in answers.response.authority[0] ])
        return ns if ns else []
