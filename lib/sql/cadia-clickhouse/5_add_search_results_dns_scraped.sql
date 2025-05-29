-- Column to track when DNS was last scraped for each domain
ALTER TABLE cadia.search_results
ADD COLUMN dns_scraped Nullable(DateTime64(3));
