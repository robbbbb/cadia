from attr import attrs, attrib, validators
from utils import all_cols
from math import ceil

def validate_order(instance, attribute, value):
    if value not in ( 'asc', 'desc' ):
        raise ValueError()

def validate_order_by(instance, attribute, value):
    if value not in all_cols:
        raise ValueError()

@attrs
class Pagination:
    order_by: str = attrib(default='visits_12m',validator=validate_order_by)
    order = attrib(default='desc',validator=validate_order)
    limit = attrib(default=100,validator=validators.instance_of(int))
    offset = attrib(default=0,validator=validators.instance_of(int))
    rows = attrib(default=None,validator=validators.optional(validators.instance_of(int)))

    @classmethod
    def parse(cls, value):
        if not value:
            return Pagination()
        parts = value.split('/')
        if len(parts) != 4:
            raise ValueError()
        return Pagination(order_by=parts[0], order=parts[1], limit=int(parts[2]), offset=int(parts[3]))

    def to_string(self):
        return "%s/%s/%d/%d" % ( self.order_by, self.order, self.limit, self.offset )
    
    def copy(self):
        return Pagination(order_by=self.order_by, order=self.order, limit=self.limit, offset=self.offset, rows=self.rows)

    def change(self, order_by=None, order=None, limit=None, offset=None):
        p = self.copy()
        if order_by is not None:
            p.order_by = order_by
        if order is not None:
            p.order = order
        if limit is not None:
            p.limit = limit
        if offset is not None:
            p.offset = offset
        return p

    def next_page(self):
        p = self.copy()
        if p.rows is None:
            raise ValueError("rows must be set before calling next_page")
        p.offset += p.limit
        if p.offset > p.rows:
            p.offset = int(p.rows/p.limit) * p.limit
        return p

    def previous_page(self):
        p = self.copy()
        p.offset -= p.limit
        if p.offset < 0:
            p.offset = 0
        return p

    def pages(self):
        # List of (pagenum,offset,active) to display in pagination
        # limit the number of items to keep it reasonable
        # The list may also contain Nones which indicate to display an ellipsis
        max_to_show = 20
        show_surrounding_pages = 4 # 4 pages at each end, and before/after current

        pages_to_show = set()
        current_page = int(self.offset / self.limit) + 1 # +1 to make pages 1-based
        last_page = ceil(self.rows / self.limit) + 1 # +1 for 1-based
        if last_page > max_to_show:
            for page in range(1,show_surrounding_pages+1):
                pages_to_show.add(page)
            for page in range(max(1,current_page-show_surrounding_pages), min(last_page, current_page+show_surrounding_pages)+1):
                pages_to_show.add(page)
            for page in range(last_page - show_surrounding_pages, last_page):
                pages_to_show.add(page)
        else:
            # simple, show all of them
            pages_to_show = set(range(1,last_page))

        last = None
        results = []
        for page in sorted(pages_to_show):
            if last is not None and last != page-1:
                results.append( None ) # Placeholder to insert an ellipsis
            results.append( (page, (page-1)*self.limit, page==current_page ) )
            last = page
        print(results)
        return results

