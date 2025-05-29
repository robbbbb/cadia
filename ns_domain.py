from utils import clickhouse_conn


class NSDomain:
    
    def ns_domain_report():
        # Report showing domains that are available and serve as NS for many other domains
        # We want to identify such domains because obtaining that single NS domain gets us the traffic of all domains pointed at it

        conn = clickhouse_conn()
        # The "having count(*) > 1" is to ignore domains that are only NS for themself
        sql = "select ns, count(*) from (select arrayJoin(ns_domains) as ns from domains) d1 join domains d2 on d1.ns=d2.domain and d2.registered=false group by 1 having count(*) > 1 order by 2 desc"
        rows = conn.execute(sql)
        result = [dict(zip(['ns', 'count'], row)) for row in rows]
        return result
