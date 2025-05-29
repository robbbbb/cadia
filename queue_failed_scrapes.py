from utils import clickhouse_conn, queue_scrape


conn = clickhouse_conn()

sql = "select distinct domain, scraper from errors where timestamp >= now() - interval '1 day' and error_type in ('SSLError','ProxyError','ReadTimeout','HTTPError');"
rows = conn.execute(sql)
results = [dict(zip(['domain', 'scraper'], row)) for row in rows]


# Iterate through results based on scraper type and queue scrapes
for fail_scrape in results:
    queue_scrape(
        domain=fail_scrape["domain"],
        scraper=fail_scrape["scraper"],
        producer=None
    )
print("Done scraping failed domains")