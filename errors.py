from typing import LiteralString
from utils import clickhouse_conn


class Errors:
    def recent_errors(self):
        ch = clickhouse_conn()
        errors_cols = ['uuids', 'error_type', 'file',
                       'line', 'scraper', 'topic', 'domains', 'count', 'latest']
        sql = """select groupArray(10)(uuid) AS uuids, error_type, 
        file, line, scraper, topic, groupArray(10)(domain) AS domains, 
        count(*) AS count, max(timestamp) as latest from errors
        WHERE  timestamp > now() - interval '3 day' 
        group by error_type, file, line, scraper, topic order by count desc
        """

        rows = ch.execute(sql)
        results = [dict(zip(errors_cols, row)) for row in rows]
        return results

    def latest(self):
        ch = clickhouse_conn()
        errors_cols = ['uuid', 'error_type', 'error', 'file',
                       'line', 'scraper', 'domain', 'timestamp' ]
        sql = """select uuid, error_type, error,
        file, line, scraper, domain, timestamp
        from errors
        order by timestamp desc
        limit 100
        """

        rows = ch.execute(sql)
        results = [dict(zip(errors_cols, row)) for row in rows]
        return results

    def error_details(self, uuid) -> list[dict]:
        ch = clickhouse_conn()
        errors_cols: list[str] = ['domain', 'error', 'error_type', 'file',
                                  'line', 'scraper', 'topic', 'timestamp', 'traceback', 'uuid']
        sql: LiteralString = f"""select *
            from errors where uuid = '{uuid}'"""

        rows = ch.execute(sql)
        results = [dict(zip(errors_cols, row)) for row in rows]
        return results
