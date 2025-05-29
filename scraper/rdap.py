# RDAP scraper
from scraper.base import BaseScraper, ThrottledException
from result import RDAPResult, RDAPNXResult
from utils import tld
from time import time
import json
import requests
import threading
import os
import publicsuffix2

class RDAPScraper(BaseScraper):
    tld_urls = {}
    url_shortname = {}
    session = requests.Session()

    def __init__(self):
        if not RDAPScraper.tld_urls:
            # Data from https://data.iana.org/rdap/dns.json
            print("Reading RDAP servers from dns.json")
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            rel_path = "../data/dns.json"
            abs_file_path = os.path.join(script_dir, rel_path)
            
            with threading.Lock():
                with open(abs_file_path) as f:
                    rdap_list = json.loads(f.read())
                for item in rdap_list["services"]:
                    tlds = item[0]
                    url = item[1][0] # All currently have single URL
                    for tld in tlds:
                        RDAPScraper.tld_urls[tld] = url
                    # Short name that can be used in a kafka topic
                    # "https://rdap.verisign.com/com/v1/" -> "rdap_verisign_com_com_v1"
                    RDAPScraper.url_shortname[url] = url.lstrip('https://').rstrip('/').replace('.','_').replace('/','_').replace(':','_')

    # TODO get https://data.iana.org/rdap/dns.json

    def scrape_domain(self,domain,proxy=None):
        
        if proxy:
            proxy_url = f"socks5://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            proxies = { "http": proxy_url, "https": proxy_url }
        else:
            proxies = None

        rdap_url = RDAPScraper.tld_urls[tld(domain)]
        #print("RDAP scrape %s..." % domain)
        r = self.session.get(rdap_url + 'domain/' + domain, timeout=10, proxies=proxies)
        tlds = publicsuffix2.get_tld(domain)
        rdap_raw = r.text
        #print(r.text)
        # fetching of related link is disabled for now because they're so unreliable. need to track failures and implement retries
        #if tlds in ['com', 'net']:
        #    rdap_raw = self.get_related_link(r.text)
        
        #print("RDAP scrape %s done" % domain)
        if r.status_code == 200:
            return RDAPResult(domain=domain, registered=True, rdap_json=r.text, **self.parse_response(r.text), **self.parse_contacts(rdap_raw))
        if r.status_code == 404:
            # Different result type for non-existant domains to avoid setting expiry etc to null
            #print("404 for domain %s. Returning NX" % domain)
            return RDAPNXResult(domain=domain, registered=False)
        elif r.status_code in ( 429, 422, 403 ):
            # handle 429 / 403 status for too many requests
            # 422 seems to be used to indicate too many requests but isn't standard
            #self.set_backoff(rdap_url)
            #print("Got throttled, backing off")
            raise ThrottledException()
        else:
            r.raise_for_status()
        #response=r.json()

    def shard_for_domain(self,domain):
        try:
            return RDAPScraper.url_shortname[RDAPScraper.tld_urls[tld(domain)]]
        except (ValueError, KeyError):
            # No valid TLD in domain, or no RDAP server for that TLD
            return None


    backoffs = {}

    def set_backoff(self,url,seconds=10):
        with threading.Lock():
            RDAPScraper.backoffs[url] = time() + seconds

    def should_backoff(self,url):
        with threading.Lock():
            return url in RDAPScraper.backoffs and RDAPScraper.backoffs[url] > time()

    def parse_response(self,text):
        data = json.loads(text)
        
        return {
            'registration_date' : self.event_date(data, "registration"),
            # if expiration date is missing, use the soft expiration date
            'expiration_date' : self.event_date(data, "expiration") or self.event_date(data, "soft expiration"),
            'transfer_date' : self.event_date(data, "transfer"),
            'last_changed_date' : self.event_date(data, "last changed"),
            'registrar_id' : self.registrar_id(data),
            'status': self.rdap_status(data, "status"),
        }

    def parse_contacts(self, text):
        data = json.loads(text)
        contact_details = self.fetch_rdap_contacts(data)
        return {
            'registrant_name' : contact_details.get('registrant_name'),
            'registrant_organization' : contact_details.get('registrant_organization'),
            'registrant_adr' : contact_details.get('registrant_adr'),
            'registrant_state' : contact_details.get('registrant_state'),
            'registrant_country' : contact_details.get('registrant_country'),
            'registrant_phone' : contact_details.get('registrant_phone'),
            'registrant_email' : contact_details.get('registrant_email'),
            'tech_name' : contact_details.get('tech_name'),
            'tech_organization' : contact_details.get('tech_organization'),
            'tech_adr' : contact_details.get('tech_adr'),
            'tech_state' : contact_details.get('tech_state'),
            'tech_country' : contact_details.get('tech_country'),
            'tech_phone' : contact_details.get('tech_phone'),
            'tech_email' : contact_details.get('tech_email'),
            'admin_name' : contact_details.get('admin_name'),
            'admin_organization' : contact_details.get('admin_organization'),
            'admin_adr' : contact_details.get('admin_adr'),
            'admin_state' : contact_details.get('admin_state'),
            'admin_country' : contact_details.get('admin_country'),
            'admin_phone' : contact_details.get('admin_phone'),
            'admin_email' : contact_details.get('admin_email'),
        }
        

    def get_related_link(self, json_data):
        # This function will only be used when parsing the contact details
        # All the contact details are located on the related link of the first rdap response
        json_data = json.loads(json_data)
        link = [e['href'] for e in json_data['links'] if e['rel'] == 'related']
        if link:
            r = requests.get(link[0], timeout=5)
            if r.status_code == 200 :
                return r.text
            if r.status_code == 404:
                raise requests.HTTPError(response=r)
            elif r.status_code in (429, 422, 403):
                raise ThrottledException()
            else:
                r.raise_for_status()
        else:
            return None
    
    def fetch_rdap_contacts(self, rdap_raw):
        # set all contact details to None or empty lists
        contacts = {
            'registrant_organization': None,
            'registrant_adr': [],  # Ensure this is an empty list
            'registrant_state': None,
            'registrant_country': None,
            'registrant_name': None,
            'registrant_phone': None,
            'registrant_email': None,
            'admin_organization': None,
            'admin_adr': [],  # Ensure this is an empty list
            'admin_state': None,
            'admin_country': None,
            'admin_name': None,
            'admin_phone': None,
            'admin_email': None,
            'tech_organization': None,
            'tech_adr': [],  # Ensure this is an empty list
            'tech_state': None,
            'tech_country': None,
            'tech_name': None,
            'tech_phone': None,
            'tech_email': None
        }

        for entity in rdap_raw.get('entities', []):
            # Check each entity and obtain its role and vcardArray
            roles = entity.get('roles', [])
            vcard = entity.get('vcardArray', [None, []])[1]
            domain = entity.get('handle', '')

            # Extract necessary contact details if available
            org = [e[-1] for e in vcard if e[0] == 'org' and len(e) > 0]
            name = [e[-1] for e in vcard if e[0] == 'fn' and len(e) > 0]
            
            # Some domains have their telephone on [1] such as .info while domains
            # Stay consistent on [-1]
            tel = [
                e[1] if (domain.endswith('.info') and len(e) > 1) else e[-1] 
                for e in vcard if e[0] == 'tel' and len(e) > 0
            ]
            email = [e[-1] for e in vcard if e[0] == 'email' and len(e) > 0]
            adr = [e[-1] for e in vcard if e[0] == 'adr' and len(e) > 0]

            # Obtain specific details in the address such as Country and State
            if adr:
                adr_filtered = [a for a in adr if a]
                if adr_filtered:
                    # Reverse the address to ensure the correct elements are accessed
                    adr_reversed = [a[::-1] for a in adr_filtered]
                    country = adr_reversed[0][0] if len(adr_reversed[0]) > 0 else None
                    state = adr_reversed[0][2] if len(adr_reversed[0]) > 2 else None
                else:
                    country, state = None, None
            else:
                country, state = None, None

            # Set data to the appropriate roles
            if 'registrant' in roles:
                contacts['registrant_organization'] = org[0] if org else contacts['registrant_organization']
                contacts['registrant_name'] = name[0] if name else contacts['registrant_name']
                contacts['registrant_phone'] = tel[0] if tel else contacts['registrant_phone']
                contacts['registrant_email'] = email[0] if email else contacts['registrant_email']
                contacts['registrant_adr'] = adr if adr else contacts['registrant_adr']
                contacts['registrant_state'] = state if state else contacts['registrant_state']
                contacts['registrant_country'] = country if country else contacts['registrant_country']
            
            if 'administrative' in roles or any(role.startswith('admin') for role in roles):
                contacts['admin_organization'] = org[0] if org else contacts['admin_organization']
                contacts['admin_name'] = name[0] if name else contacts['admin_name']
                contacts['admin_phone'] = tel[0] if tel else contacts['admin_phone']
                contacts['admin_email'] = email[0] if email else contacts['admin_email']
                contacts['admin_adr'] = adr if adr else contacts['admin_adr']
                contacts['admin_state'] = state if state else contacts['admin_state']
                contacts['admin_country'] = country if country else contacts['admin_country']
            
            if 'technical' in roles or any(role.startswith('tech') for role in roles):
                contacts['tech_organization'] = org[0] if org else contacts['tech_organization']
                contacts['tech_name'] = name[0] if name else contacts['tech_name']
                contacts['tech_phone'] = tel[0] if tel else contacts['tech_phone']
                contacts['tech_email'] = email[0] if email else contacts['tech_email']
                contacts['tech_adr'] = adr if adr else contacts['tech_adr']
                contacts['tech_state'] = state if state else contacts['tech_state']
                contacts['tech_country'] = country if country else contacts['tech_country']

        return contacts

    
    def event_date(self,data,event):
        # Events (registration etc) are stored in an array of hashes eg:
        #  [ { "eventAction": "registration", "eventDate": "2006-08-18T02:05:16.65Z" }, ... ]
        # Finds the matching event and returns the date
        # TODO add a test for missing dates
        dates = [e["eventDate"] for e in data["events"] if e["eventAction"] == event ]
        if dates:
            return dates[0]
        else:
            return None

    def registrar_id(self,data):
        
        # workaround for different capitalisation of "identifier" in different RDAP responses
        def _get_identifier(i):
            try:
                return i["identifier"]
            except KeyError:
                return i["Identifier"]  

        # registrar id may be in top level "publicIds" (seen in donuts) or in "entities" with "registrar" role (seen in verisign)
        if "publicIds" in data.keys():
            ids = [i["identifier"] for i in data["publicIds"] if i["type"] == "IANA Registrar ID"]
            if ids:
                return int(ids[0])
             
        entities = [e for e in data["entities"] if "registrar" in e["roles"]]

        for entity in entities:
            # workaround for different capitalisation of "publicIds" in different RDAP responses
            # also publicIds or publicIDs is in a list of hashes
            if "publicIds" in entity.keys() or "publicIDs" in entity.keys():
                # workaround - .click (uniregistry) has incorrect capitilisation "publicIDs", in violation of the rfc
                public_ids = entity["publicIds"] if "publicIds" in entity else entity["publicIDs"] 
                ids = [_get_identifier(i) for i in public_ids if i["type"] == "IANA Registrar ID"]
                if ids:
                    return int(ids[0])
        return None

    def rdap_status(self, data, key):
        if key not in data:
            return []
        status = data[key]
        if isinstance(status, list):
            return status
        else:
            return [status]
