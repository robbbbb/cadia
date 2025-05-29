from zonefile import parse_zone_file, read_zonefile_incremental

def find_domain(domain,arr):
    try:
        return [ d for d in arr if d[0] == domain ][0]
    except IndexError:
        return None

def test_parse_zonefile():
    domains = list(parse_zone_file('tests/data/sample-zone-1.gz','com'))
    assert len(domains) == 3
    #above = [ d for d in domains if d[0] == 'above.com' ][0]
    above = find_domain('above.com', domains)
    assert above[1] == [ 'ns1.trellian.com', 'ns2.trellian.com' ]
    assert find_domain('0--3.com', domains) is None

def test_parse_zonefile_relative():
    # This zone doesn't include the TLD on every line, they are assumed relative to the tld/origin
    # It's also in all caps. Verisign used this format in the past
    domains = list(parse_zone_file('tests/data/sample-zone-3.gz','com'))
    assert len(domains) == 11
    trellian = find_domain('trellian.com', domains)
    assert trellian[1] == [ 'ns1.trellian.com', 'ns2.trellian.com' ]
    assert find_domain('0--3.com', domains) is None

def test_read_zonefile_incremental():
    changes = list(read_zonefile_incremental('tests/data/sample-zone-2.gz','tests/data/sample-zone-1.gz','com'))
    assert changes
    # should be: above.com removed
    above = find_domain('above.com', changes)
    assert above[1] == []
    # addme.com added
    addme = find_domain('addme.com', changes)
    assert addme[1] == [ 'ns1.trellian.com', 'ns2.trellian.com' ]
    # xyz.com ns changed
    xyz = find_domain('xyz.com', changes)
    assert xyz[1] == [ 'ns0.somedns.com', 'ns3.somedns.com' ]
    #print(changes)

def test_read_zonefile_incremental2():
    # incremental import between the same file should give empty result
    changes = list(read_zonefile_incremental('tests/data/sample-zone-3.gz','tests/data/sample-zone-3.gz','com'))
    assert len(changes) == 0

