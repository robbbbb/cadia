import pytest
from pagination import Pagination

def test_pagination_validation():
    p = Pagination()
    p = Pagination(order_by='visits_12m', order='desc', limit=50, offset=0)
    with pytest.raises(ValueError):
        p = Pagination(order='nonsense')
    with pytest.raises(ValueError):
        p = Pagination(order_by='nonexistant_column')

def test_parse():
    pag_str = 'visits_12m/desc/50/0'
    p = Pagination.parse(pag_str)
    assert p.order_by == 'visits_12m'
    assert p.order == 'desc'
    assert p.limit == 50
    assert p.offset == 0

    assert p.to_string() == pag_str

def test_change_order():
    p = Pagination(order_by='domain', order='asc')
    p2 = p.change(order_by='registered', order='desc')
    assert p2.order_by == 'registered'
    assert p2.order == 'desc'

def test_next_page():
    p = Pagination(order_by='domain', order='asc', limit=50, offset=0, rows=70)
    p2 = p.next_page()
    assert p2.offset == 50
    p2 = p2.next_page() # Shouldn't go past end of rows
    assert p2.offset == 50

    p = Pagination(order_by='domain', order='asc', limit=50, offset=50)
    p2 = p.previous_page()
    assert p2.offset == 0
    p2 = p2.previous_page() # Shouldn't go negative
    assert p2.offset == 0

def test_pages_simple():
    p = Pagination(order_by='domain', order='asc', limit=50, offset=0, rows=500)
    pages = p.pages()
    assert len(pages) == 10
    assert pages[3] == (4, 150, False)
    assert pages[0] == (1, 0, True)

def test_pages_complex():
    p = Pagination(order_by='domain', order='asc', limit=50, offset=500, rows=5000)
    pages = p.pages()
    assert len(pages) == 19 # 4 at start, ellipsis, 4 each side of current page, current page itself, ellipsis, 4 at end
    assert len([ p for p in pages if p is None ]) == 2 # 2 Nones
