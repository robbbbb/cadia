from result import Result, DNSResult, RDAPNXResult, datetime_or_iso
from utils import json_serial
import json
import attr

def test_result():
    r = Result(domain='test.com', modified_by='testing')
    j = r.serialize()
    r2 = Result.deserialize(j)
    assert r2 == r

def test_init():
    r = DNSResult(domain='test.com',rcode='NOERROR',addr=['1.2.3.4'],ns=['ns1.test.com','ns2.test.com'],mx=[],txt=[],ns_domains=['test.com'], dns_exists=True)

def test_serialize():
    r = DNSResult(domain='test.com',rcode='NOERROR',addr=['1.2.3.4'],ns=['ns1.test.com','ns2.test.com'],mx=[],txt=[],ns_domains=['test.com'], dns_exists=True)
    #j = json.dumps(r.asdict(), default=json_serial)
    j = r.serialize()
    print(j)
    #r2 = DNSResult(**json.loads(j))
    r2 = Result.deserialize(j)
    assert r2 == r
    print(r2)

def test_datetime_or_iso():
    datetime_or_iso('2006-08-18T02:05:16.65Z')

def test_lower():
    # domains should automatically convert to lower case
    r = DNSResult(domain='TeSt.com',rcode='NOERROR',addr=['1.2.3.4'],ns=['ns1.test.com','ns2.TEST.com'],mx=[],txt=[],ns_domains=['test.com'], dns_exists=True)
    assert r.domain == 'test.com'
    assert r.ns == [ 'ns1.test.com', 'ns2.test.com' ]

def test_make_result():
    # Verify that it creates the correct type
    d = { 'type' : 'RDAPNXResult', 'modified_by' : 'rdap', 'domain' : 'foo.com', 'registered' : False }
    res = Result.fromdict(d)
    assert type(res) == RDAPNXResult
