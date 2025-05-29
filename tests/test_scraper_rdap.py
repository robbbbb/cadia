from scraper.rdap import RDAPScraper
import datetime
from dateutil.tz import tzutc
import json

def test_rdap_scraper():
    r = RDAPScraper().scrape_domain('trellian.info')
    assert r.registered

def test_rdap_parser_info():
    with open("tests/data/trellian-info-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registration_date"] == '2006-08-18T02:05:16.65Z'
    assert r["expiration_date"] == '2023-08-18T02:05:16.65Z'
    assert r["transfer_date"] == '2010-01-20T21:16:00.65Z'
    assert r["last_changed_date"] == '2022-08-17T02:02:06.521Z'
    assert r["registrar_id"] == 940

def test_rdap_contact_info():
    with open("tests/data/trellian-info-rdap.json") as f:
        rdap_json = json.load(f)

    r = RDAPScraper().fetch_rdap_contacts(rdap_json)

    assert r["registrant_organization"] == 'Above.com Domain Privacy'
    assert r["registrant_state"] == 'VIC'
    assert r["registrant_country"] == 'AU'


def test_rdap_parser_com():
    with open("tests/data/trellian-com-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registrar_id"] == 940

def test_rdap_scraper_net():
    r = RDAPScraper().scrape_domain('speedtest.net')
    assert r.registered
    
def test_rdap_parser_net():
    with open("tests/data/speedtest-net-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registration_date"] == '1999-06-25T05:27:48Z'
    assert r["expiration_date"] == '2032-06-25T05:27:48Z'
    assert r["last_changed_date"] == '2022-08-17T11:48:24Z'
    assert r["registrar_id"] == 299

def test_rdap_contact_net():
    with open("tests/data/speedtest-net-contact-rdap.json") as f:
        rdap_json = json.load(f)

    r = RDAPScraper().fetch_rdap_contacts(rdap_json)

    assert r["registrant_name"] == 'Corporate Counsel'
    assert r["registrant_organization"] == 'Ookla LLC'
    assert r["registrant_state"] == 'WA'
    assert r["registrant_country"] == None
    assert r["registrant_phone"] == 'tel:+1.2062601250'
    assert r["registrant_email"] == 'hostmaster@ziffdavis.com'
    assert r["tech_name"] == 'DNS Administrator'
    assert r["tech_organization"] == 'CSC Corporate Domains, Inc.'
    assert r["tech_state"] == 'DE'
    assert r["tech_country"] == None
    assert r["tech_phone"] == 'tel:+1.3026365400'
    assert r["tech_email"] == 'dns-admin@cscglobal.com'
    assert r["admin_name"] == 'Corporate Counsel'
    assert r["admin_organization"] == 'Ookla LLC'
    assert r["admin_state"] == 'WA'
    assert r["admin_country"] == None
    assert r["admin_phone"] == 'tel:+1.2062601250'
    assert r["admin_email"] == 'hostmaster@ziffdavis.com'

def test_rdap_scraper_com():
    r = RDAPScraper().scrape_domain('amazon.com')
    assert r.registered
    
def test_rdap_parser_amazon_com():
    with open("tests/data/amazon-com-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registration_date"] == '1994-11-01T05:00:00Z'
    assert r["expiration_date"] == '2024-10-31T04:00:00Z'
    assert r["last_changed_date"] == '2023-05-16T19:03:14Z'
    assert r["registrar_id"] == 292

def test_rdap_contact_com():
    with open("tests/data/amazon-com-contact-rdap.json") as f:
        rdap_json = json.load(f)

    r = RDAPScraper().fetch_rdap_contacts(rdap_json)

    assert r["registrant_name"] == 'Hostmaster, Amazon Legal Dept.'
    assert r["registrant_organization"] == 'Amazon Technologies, Inc.'
    assert r["registrant_state"] == 'NV'
    assert r["registrant_country"] == 'US'
    assert r["registrant_phone"] == '+1.2062664064'
    assert r["registrant_email"] == 'hostmaster@amazon.com'
    assert r["tech_name"] == 'Hostmaster, Amazon Legal Dept.'
    assert r["tech_organization"] == 'Amazon Technologies, Inc.'
    assert r["tech_state"] == 'NV'
    assert r["tech_country"] == 'US'
    assert r["tech_phone"] == '+1.2062664064'
    assert r["tech_email"] == 'hostmaster@amazon.com'
    assert r["admin_name"] == 'Hostmaster, Amazon Legal Dept.'
    assert r["admin_organization"] == 'Amazon Technologies, Inc.'
    assert r["admin_state"] == 'NV'
    assert r["admin_country"] == 'US'
    assert r["admin_phone"] == '+1.2062664064'
    assert r["admin_email"] == 'hostmaster@amazon.com'
    
def test_shard():
    r = RDAPScraper()
    assert r.shard_for_domain('a.com') == 'rdap_verisign_com_com_v1'
    assert r.shard_for_domain('kis.uz') == 'rdap_cctld_uz'



# check the the identifier works with the Capitalized Identifier
def test_identifier():
    with open("tests/data/zulfa-id-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registration_date"] == '2017-09-24 19:25:32'
    assert r["expiration_date"] == '2025-09-24 23:59:59'
    assert r["last_changed_date"] == None
    assert r["registrar_id"] == 1


# if the "expiration date is not in json data use the soft expiration date"
def test_expiration_date():
    with open("tests/data/mjoifjordur-is-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registration_date"] == '2022-12-12T11:20:01+00:00'
    assert r["expiration_date"] == '2025-12-12T00:00:00+00:00'
    assert r["last_changed_date"] == '2024-10-28T11:02:20'
    assert r["registrar_id"] == None


# some tld's don't have publicIds or publicIDs
def test_registrar_id():
    with open("tests/data/fortemix-cz-rdap.json") as f:
        json = f.read()

    r = RDAPScraper().parse_response(json)
    assert r["registrar_id"] == None