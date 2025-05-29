from domain import Domain

def test_domain():
    dom = Domain('archive.org') # domain must exist in dev db
    assert dom is not None
    assert dom.exists()
    assert dom.data['domain'] == 'archive.org'

def test_nonexistant_domain():
    dom = Domain('adfoin98-32hpadsf3.com')
    assert dom is not None
    assert not dom.exists()
