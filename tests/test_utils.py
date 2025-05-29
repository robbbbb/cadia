from utils import tld, normalize, memcached_connect, report_error, db_cols, db_default_row, rdap_unsupported_tlds
from sys import exc_info

def test_tld():
    assert tld('trellian.com') == 'com'
    assert tld('trellian.co.uk') == 'co.uk'

def test_normalize():
    assert normalize('trellian.com') == 'trellian.com'
    assert normalize('abc.def.trellian.com') == 'trellian.com'
    assert normalize('trellian.co.uk') == 'trellian.co.uk'

def test_memcached():
    mc = memcached_connect()
    mc.set("cadia_pytest", 5)
    assert mc.get("cadia_pytest") == 5

def test_report_error():
    try:
        1/0
    except Exception as e:
        report_error('testreporterror.com', 'just a test', 'JustTestingError', exc_info()[2], 'none', 'topic')

def test_get_cols():
    cols = db_cols()

    assert 'last_modified' in cols

def test_get_default_row():
    row = db_default_row()

    assert row['modified_by'] == ''
    assert row['registration_date'] is None
    assert row['ns'] == []

def test_rdap_unsupported_tlds():
    tlds = rdap_unsupported_tlds()
    assert tlds
    assert 'com.au' in tlds
