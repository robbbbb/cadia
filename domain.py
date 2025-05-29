from utils import clickhouse_conn, all_cols

class Domain:
    def __init__(self, domain):
        self.domain = domain
        self.data = None
        self.load_from_db()

    def load_from_db(self):
        ch = clickhouse_conn()
        cols = ','.join(all_cols)
        sql = f"SELECT {cols} FROM domains final WHERE domain=%(domain)s"
        
        rows = ch.execute(sql, {'domain': self.domain})
        if rows:
            self.data = dict(zip(all_cols, rows[0]))

    def exists(self):
        return self.data is not None


'''
The History class represents the history of changes for a domain
provides methods to load historical data from the database and check 
its existence.

'''
class History:
    def __init__(self, domain):
        self.domain = domain
        self.data = None
        self.load_from_db()

    def load_from_db(self):
        ch = clickhouse_conn()
        cols = ','.join(all_cols)
        sql = f"SELECT {cols} FROM history WHERE domain=%(domain)s ORDER BY last_modified DESC"
        
        rows = ch.execute(sql, {'domain': self.domain})
        if rows:
            self.data = []
            previous_row = None
            for row in reversed(rows):  # Process from oldest to latest
                history_entry = dict(zip(all_cols, row))
                changed_fields = {
                    key: value for key, value in history_entry.items()
                    if previous_row is None or previous_row[key] != value
                }
                if changed_fields:
                    self.data.append(changed_fields)
                previous_row = history_entry

    def exists(self):
        return self.data is not None
