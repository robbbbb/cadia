# Scraper plugin base class
# To write a scraper, inherit this class and implement scrape_domain and optionally shard_for_domain
class BaseScraper:
    # scrape_domain must return a dict containing column:value to update
    # or throw an exception if the request could not be performed
    def scrape_domain(self,domain,proxy=None):
        # no-op
        return {}

    def shard_for_domain(self,domain):
        # Allows for sharding request queues eg per RDAP endpoint
        # so down/throttled endpoints don't affect others
        # Default is all in one queue
        return None

class ThrottledException(Exception):
    def __init__(self,message="Throttled"):
        super().__init__(message)
