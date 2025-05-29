from update import enrich

def test_enrich_lookup():
    new = {
        'domain': 'test.com',
        'modified_by': 'dns',
        'addr': ['8.8.8.8', '103.62.152.106'], # testing public ip
        'dns_exists': True,
        'registered': False
    }
    enrich({}, new)

    # assert that the new values are updated for geo and asn
    assert new['geo'] == ['PH', 'US'] 
    assert new['asn'] == ['134788', '15169']


def test_enrich_lookup_none():
    new = {
        'domain': 'test.com',
        'modified_by': 'dns',
        'addr': ['10.0.0.0'], # testing private ip
        'dns_exists': True,
        'registered': False
    }

    enrich({}, new)

    # assert that the new values are updated for geo and asn
    assert new['geo'] == [] 
    assert new['asn'] == []


def test_enrich_lookup_combine_ip():
    new = {
        'domain': 'test.com',
        'modified_by': 'dns',
        'addr': ['10.0.0.0', '1.1.1.1', '8.8.8.8', '103.62.152.106', '192.168.1.1'], # testing private ip and public ip (ph and google.com, cloudflare.com)
        'dns_exists': True,
        'registered': False
    }

    enrich({}, new)

    # assert that the new values are updated for geo and asn
    assert new['geo'] == ['PH', 'US'] 
    assert new['asn'] == ['13335', '134788', '15169']
