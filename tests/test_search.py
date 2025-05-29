from search import Search
from pagination import Pagination

def test_search():
    s = Search()
    assert s is not None

    s.add('ns_domains','exact','above.com')
    s.add('domain','substring','test')
    sql, param = s.sql()

    assert 'AND' in sql
    assert 'domain like' in sql

def test_search_results():
    s = Search()
    s.add('ns_domains','exact','above.com')
    #s.add('tld','exact','shop')
    s.start_search()

    results = s.results(Pagination(limit=1,order_by='domain'))
    assert results
    assert len(results) == 1

    results = s.results(Pagination(limit=1,offset=2,order_by='last_modified',order='desc'))
    assert results

def test_save_load():
    s = Search()
    s.add('tld','exact','net')
    s.start_search()

    s2 = Search(s.id)
    assert(s2.criteria)
    assert(s2.criteria[0] == ('tld','exact','net'))
