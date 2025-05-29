from utils import postgres_conn

class Tag:
    def __init__(self, id=None, search_id=None):
        if id:
            self.load_by_id(id)
        elif search_id:
            self.load_by_search_id(search_id)

    def load_by_id(self,id):
        db = postgres_conn()
        cur = db.cursor()
        cur.execute("select id, search_id, name, colour, icon, notes from tags where id=%s", (id,))
        row = cur.fetchone()
        self.id, self.search_id, self.name, self.colour, self.icon, self.notes = row

    def load_by_search_id(self,search_id):
        db = postgres_conn()
        cur = db.cursor()
        cur.execute("select id, search_id, name, colour, icon, notes from tags where search_id=%s", (search_id,))
        row = cur.fetchone()
        if row:
            self.id, self.search_id, self.name, self.colour, self.icon, self.notes = row
