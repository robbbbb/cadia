from result import DNSResult
from scraper.dns import DNSScraper

def test_dns_scraper():
    d = DNSScraper()
    r: DNSResult = d.scrape_domain('trellian.com')
    assert r.ns == [ 'ns1.trellian.com', 'ns2.trellian.com' ]
    assert r.dns_exists

    # Parking dns - check that we get the NS according to the registry not the host's dns server
    r = d.scrape_domain('cab.com')
    assert r.ns == [ 'ns3.abovedomains.com', 'ns4.abovedomains.com' ]

def test_dns_nonexistant():
    d = DNSScraper()
    r: DNSResult = d.scrape_domain('jsdhbf89ashdbfjdfsa.info')
    assert not r.dns_exists

# Test case to reproduce .co.jp errors
def test_co_jp():
    # DNSScraper.resolver_for_tld returned None for co.jp
    assert DNSScraper.resolver_for_tld('jp')
    assert DNSScraper.resolver_for_tld('co.jp')

    d = DNSScraper()
    r = d.scrape_domain('sony.co.jp')
    assert r.dns_exists

def test_punycode_tld():
    domain = 'xn--80aamlgmpeok5n.xn--p1ai'.encode().decode('idna')
    r = DNSScraper().scrape_domain(domain)

    assert r.dns_exists


# ns ns1.cheaphost.com.bd and our tld list does not include com.bd
# test ns_domains return empty list
# test ns_domains is not None
def test_ns_domain_none_type():
    d = DNSScraper()
    r: DNSResult = d.scrape_domain('updfcht.com')
    assert r.ns_domains == []
    assert r.ns_domains is not None or r.ns_domains != [None]