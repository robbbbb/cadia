from scraper.whois import WHOISScraper
import datetime
import json

def test_whois_scraper():
    r = WHOISScraper().scrape_domain('trellian.info')
    assert r.registered

# Whois is only used for TLDs that don't implement RDAP. The tests below cover those TLDs

def test_whois_parser_au():
    with open("tests/data/peake-com-au-whois.txt") as f:
        text = f.read()

    r = WHOISScraper().parse_response(text, 'peake.com.au')
    # Registrar ID is mapped from registrar name in au whois
    assert r.registrar_name == 'Synergy Wholesale Accreditations Pty Ltd'
    assert r.registrar_id == 1609

    # registrant contact name
    assert r.registrant_name == 'Anthony Peake'

    # actual registrant and eligibility id, specific for au
    assert r.registrant_id == 'ABN 53579069155'
    assert r.registrant == 'PARKER, NATHAN JAMES ROBERT'

    assert r.status == ['serverRenewProhibited']

def test_whois__with_eligibility_au():
    with open("tests/data/flexisourceit-com-au-whois-eligibility.txt") as f:
        text = f.read()
    r = WHOISScraper().parse_response(text, 'flexisourceit.com.au')
    assert r.eligibility_type == 'Company'
    assert r.eligibility_name == 'Flexisource IT Trust'
    assert r.eligibility_id == 'ABN 67818609729'

def test_whois_parser_us():
    with open("tests/data/techstartups-us-raw.txt") as f:
        text = f.read()
    
    r = WHOISScraper().parse_response(text, 'techstartups.us')
    assert r.registered
    assert r.registration_date == datetime.datetime(2020, 7, 15, 12, 34, 23, tzinfo=datetime.UTC)
    assert r.expiration_date == datetime.datetime(2025, 7, 15, 12, 34, 23, tzinfo=datetime.UTC)
    assert r.last_changed_date == datetime.datetime(2024, 7, 25, 7, 29, 26, tzinfo=datetime.UTC)
    assert r.registrar_id == 472
    assert r.status == ['clientTransferProhibited']

def test_whois_nonexistant_domain():
    for file in ( "tests/data/nonexistant-whois-us.txt", "tests/data/nonexistant-whois-au.txt" ):
        with open("tests/data/nonexistant-whois-us.txt") as f:
            text = f.read()
        r = WHOISScraper().parse_response(text, 'whatever')
        assert not r.registered
        assert type(r).__name__ == 'WHOISNXResult'

# TODO this is unused and should be deleted unless we find a need for it
def test_whois_date_formatter():
    r = WHOISScraper()
    assert r.format_whois_date('2018-02-21T12:34:56Z') == '2018-02-21 12:34:56+0000'
    assert r.format_whois_date('2018-02-21T18:36:40-0700') == '2018-02-21 18:36:40-0700'
    assert r.format_whois_date('2018-02-21T18:36:40+0700') == '2018-02-21 18:36:40+0700'
    assert r.format_whois_date('2018-01-23') == '2018-01-23 00:00:00+0000'
    assert r.format_whois_date('2018-01-23T01:23:45') == '2018-01-23 01:23:45+0000'



