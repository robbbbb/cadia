import argparse
from scraper.dns import DNSScraper
from scraper.rdap import RDAPScraper
from scraper.whois import WHOISScraper

# TODO this should be in a utility function
scrapers = { 'dns' : DNSScraper(), 'rdap' : RDAPScraper(), 'whois' :  WHOISScraper()}

parser = argparse.ArgumentParser(description='Test scraper')
parser.add_argument('--scraper', required=True, help='Which scraper to run. Eg rdap or dns')
parser.add_argument('--domain', required=True, help='Domain name to scrape')
args = parser.parse_args()

result = scrapers[args.scraper].scrape_domain(args.domain)
result.modified_by = args.scraper
print(result)

