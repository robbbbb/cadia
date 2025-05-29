from utils import clickhouse_conn

class Reports:
    def scraped():
        # Shows when domains were last scraped. Grouped by different periods

        periods = [ (None,90), (90,60), (60,30), (30,7), (7,None) ]

        conn = clickhouse_conn()
        sql = """select countIf(dns_scraped is null) as dns_scraped_never,
        countIf(dns_scraped < date_sub(now(), interval 90 day)) as dns_scraped_gt_90,
        countIf(dns_scraped between date_sub(now(), interval 90 day) and date_sub(now(), interval 60 day)) as dns_scraped_60_90,
        countIf(dns_scraped between date_sub(now(), interval 60 day) and date_sub(now(), interval 30 day)) as dns_scraped_30_60,
        countIf(dns_scraped between date_sub(now(), interval 30 day) and date_sub(now(), interval 7 day)) as dns_scraped_30_7,
        countIf(dns_scraped > date_sub(now(), interval 7 day)) as dns_scraped_lt_7,
        countIf(whois_scraped is null) as whois_scraped_never,
        countIf(whois_scraped < date_sub(now(), interval 90 day)) as whois_scraped_gt_90,
        countIf(whois_scraped between date_sub(now(), interval 90 day) and date_sub(now(), interval 60 day)) as whois_scraped_60_90,
        countIf(whois_scraped between date_sub(now(), interval 60 day) and date_sub(now(), interval 30 day)) as whois_scraped_30_60,
        countIf(whois_scraped between date_sub(now(), interval 30 day) and date_sub(now(), interval 7 day)) as whois_scraped_30_7,
        countIf(whois_scraped > date_sub(now(), interval 7 day)) as whois_scraped_lt_7
        from domains final"""
        row = conn.execute(sql,settings={'do_not_merge_across_partitions_select_final':1})[0]
        result = dict(zip([
            'dns_scraped_never','dns_scraped_gt_90','dns_scraped_60_90','dns_scraped_30_60','dns_scraped_30_7','dns_scraped_lt_7',
            'whois_scraped_never','whois_scraped_gt_90','whois_scraped_60_90','whois_scraped_30_60','whois_scraped_30_7','whois_scraped_lt_7'
        ], row))
        #print(result)
        return result
