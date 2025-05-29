from utils import clickhouse_conn, all_cols

class Stats:
    def total_domains(self):
        ch = clickhouse_conn()
        return ch.execute("select count(*) from domains final")[0][0]

    def top_n(self,col,count):
        ch = clickhouse_conn()
        if not col in all_cols:
            raise ValueError("unknown column")
        sql = f"select {col},count(*) from domains final group by 1 order by 2 desc limit {count}"
        cols = [ 'val', 'count' ]
        
        rows = ch.execute(sql)
        results = [dict(zip(cols, row)) for row in rows]
        return results

    def dns_scrapes_1d(self):
        ch = clickhouse_conn()
        return ch.execute("select count(*) from domains final where dns_scraped > now() - interval '1 day'")[0][0]
