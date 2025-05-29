from scraper.base import BaseScraper
from result import WHOISResult, WHOISNXResult
from datetime import datetime, timezone
from glob import glob
from base64 import b64encode
import json
import re
import socket
from publicsuffix2 import get_tld
import os
import csv


# Global variable to store the CSV data
_csv_data = None

# Function to read the CSV data
def get_csv_data():
    global _csv_data
    if _csv_data is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        rel_path = "../data/registrar-ids.csv"
        abs_file_path = os.path.join(script_dir, rel_path)
        with open(abs_file_path, mode='r', encoding='utf-8') as f:
            _csv_data = {row['registrar_name']: row['id'] for row in csv.DictReader(f)}

        # Combine AU registrars data. There's a copy in git for testing
        # Latest data is in /var/state, optional because not required for dev and don't want to add a dependency on drop.com.au
        # This is downloaded by drop.com.au in jobs tagged drop:afilias:download
        au_csv_files = [ os.path.join(script_dir, '../data/registrar-ids-au.csv') ]
        downloaded_files = glob('/var/state/www/www.drop.com.au/afilias/active-registrars-802215-*.csv')
        if downloaded_files:
            au_csv_files.append(sorted(downloaded_files)[-1])
        for file in au_csv_files:
            with open(file, mode='r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    _csv_data[row['registrar_name']] = row['registrar_id']

    return _csv_data



class WHOISScraper(BaseScraper):
     
    def __init__(self):
        self.urls = {}
        
        # TODO load PSL globally like the registrar id list above
        script_dir = os.path.dirname(os.path.abspath(__file__))
        rel_path = "../data/whois.json"
        abs_file_path = os.path.join(script_dir, rel_path)
        
        
        with open(abs_file_path) as f:
            whois_tlds_settings = json.loads(f.read())
        for entry in whois_tlds_settings:
            tld = entry.get('tld')
            whois_server = entry.get('whois_server')
            if tld and whois_server:
                self.urls[tld] = whois_server
             
    def scrape_domain(self, domain, proxy=None):
        tld = self.fetch_domain_tld(domain)
        whois_server = self.urls.get(tld)

        whois_text = self.query_whois_server(domain, whois_server, proxy)
        #print(whois_text)

        return self.parse_response(whois_text, domain)

    def is_nx_result(self, text):
        nomatch_strings = [
            "No match for",
            "Domain status: available",
            "Domain status: No Object Found",
            "% nothing found",
            "% No entries found",
            "No entries found in the AFNIC Database",
            "The queried object does not exist",
            "This domain has been reserved by the registry, and is not available for registration",
            "Invalid query or domain name not known",
            "The requested domain was not found in",
            "DOMAIN NOT FOUND",
            "Not found:",
            "NOT FOUND",
            "Status: free",
            "Status: Not Registered",
            "is free",
            "Status: AVAILABLE",
            "No Data Found",
            "No information available about domain name",
            "This domain name has not been registered.",
            "query_status: 220 Available",
            "Domain not found",
        ]
        return any(entry in text for entry in nomatch_strings)

    def get_registrar_id_by_name(self, registrar_name):
        """
        Private method to get registrar ID by name from the CSV file.
        """
        if not registrar_name:
            raise ValueError("Registrar name is required.")
        
        csv_data = get_csv_data()
        return csv_data[registrar_name]

    
    def parse_response(self, whois_text, domain):
        if self.is_nx_result(whois_text):
            return WHOISNXResult(domain=domain, registered=False)
    
        patterns = [
            ('last_changed_date', r'(?:Updated Date|Last Modified):\s*(.+)'),
            ('registration_date', r'(?:Registration|Creation) Date:\s*(.+)'),
            ('expiration_date', r'(?:Registrar Registration Expiration Date|Registry Expiry Date|Expiration Date):\s*(.+)'),
            # TODO missing transfer_date
            ('status', r'(?:Domain )?Status:\s*([^\s]+)'), # stop at first space, don't want url part

            ('registrar_name', r'(?:Registrar Name|Registrar):\s*(.+)'),
            ('registrar_id', r'Registrar IANA ID:\s*(.+)'),

            ('registrant', r'Registrant:\s*(.+)'),
            ('registrant_id', r'Registrant ID:\s*(.+)'),

            ('registrant_name', r'Registrant (?:Contact )?Name:\s*(.+)'),
            ('registrant_organization', r'Registrant Organization:\s*(.+)'),
            ('registrant_stateprovince', r'Registrant State/Province:\s*(.+)'),
            ('registrant_country', r'Registrant Country:\s*(.+)'),
            ('registrant_phone', r'Registrant Phone:\s*(.+)'),
            ('registrant_email', r'Registrant Email:\s*(.+)'),

            ('admin_name', r'Admin (?:Contact )?Name:\s*(.+)'),
            ('admin_organization', r'Admin Organization:\s*(.+)'),
            ('admin_stateprovince', r'Admin State/Province:\s*(.+)'),
            ('admin_country', r'Admin Country:\s*(.+)'),
            ('admin_phone', r'Admin Phone:\s*(.+)'),
            ('admin_email', r'Admin Email:\s*(.+)'),

            ('tech_name', r'Tech (?:Contact )?Name:\s*(.+)'),
            ('tech_organization', r'Tech Organization:\s*(.+)'),
            ('tech_stateprovince', r'Tech State/Province:\s*(.+)'),
            ('tech_country', r'Tech Country:\s*(.+)'),
            ('tech_phone', r'Tech Phone:\s*(.+)'),
            ('tech_email', r'Tech Email:\s*(.+)'),

            # eligibility for .au
            ('eligibility_type', r'Eligibility Type:\s*(.+)'),
            ('eligibility_name', r'Eligibility Name:\s*(.+)'),
            ('eligibility_id', r'Eligibility ID:\s*(.+)'),
        ]

        data = { 'domain': domain, 'whois_text': whois_text, 'registered' : True }
        for line in whois_text.splitlines():
            #print(line)
            for field, regexp in patterns:
                match = re.match(regexp, line.strip())
                if match:
                    value = match[1]
                    #print(f"MATCH: {field} '{regexp}' '{line}'")
                    if field == 'status':
                        # status is List[str]. append
                        if field not in data:
                            data[field] = []
                        data[field].append(value)
                    else:
                        data[field] = value

        if 'registrar_name' in data and 'registrar_id' not in data:
            data['registrar_id'] = self.get_registrar_id_by_name(data['registrar_name'])

        return WHOISResult(**data)

    # TODO not currently called. delete unless needed for the TLDs we're using whois for
    def is_valid_date(self, date_str):
        formats = [
            '%d.%m.%Y',
            '%Y. %m. %d.',
            '%Y.%m.%d',
            '%Y-%m-%dT%H:%M:%S%z',  # Timestamp with timezone
            '%Y-%m-%dT%H:%M:%SZ',   # GMT timestamp with 'Z'
            '%Y-%m-%d',             # Date without timestamp
            '%Y-%m-%dT%H:%M:%S',    # Timestamp without timezone
        ]
        for fmt in formats:
            try:
                if "%z" in fmt:
                    # Parse date with timezone info
                    parsed_date = datetime.strptime(date_str, fmt)
                else:
                    # Parse date and set timezone to UTC
                    parsed_date = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                return parsed_date
            except ValueError:
                continue
        return None

    # TODO not currently called. delete unless needed for the TLDs we're using whois for
    def format_whois_date(self, date_str):
        parsed_date = self.is_valid_date(date_str)
        if parsed_date:
            # Format the date to include timezone offset and always in UTC/GMT
            return parsed_date.strftime('%Y-%m-%d %H:%M:%S%z')
        return ''
                   
    def query_whois_server(self, domain, whois_server, proxy):
        if proxy is None:
            raise RuntimeError("whois scraping without proxy is not implemented")
            # TODO support that option

        # Using proxies to send requests from more source addresses to work around rate limits
        # prep auth header: Proxy-Authorization: Basic base64("username:password")
        cred = b64encode(f"{proxy.username}:{proxy.password}".encode('ascii')).decode()
        auth_header = f"Proxy-Authorization: Basic {cred}"

        # connect to proxy and authenticate
        sock = socket.create_connection((proxy.host,proxy.port))
        sock.settimeout(20)
        req = f"CONNECT {whois_server}:43 HTTP/1.1\nHost: {whois_server}\n{auth_header}\n\n"
        sock.sendall(req.encode('ascii'))
        resp = sock.recv(1*1024*1024)

        if '200' not in resp.decode():
            # TODO parse it properly
            # TODO may be throttled. how to tell? "502 Bad Gateway"?
            raise RuntimeError(f"Connecting to {whois_server} via proxy failed. Reply was: '{resp.decode()}'")

        #with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        #    sock.connect((whois_server, 43))
        sock.sendall((domain + "\r\n").encode('utf-8'))
        response = b''
        while True:
            data = sock.recv(4096)
            if not data:
                break
            response += data
        return response.decode('utf-8')
       
    def extract_registrar_server(self, whois_data):
        match = re.search(r'Registrar WHOIS Server:\s*(\S+)', whois_data, re.IGNORECASE)
        if match:
            whois_server = match.group(1)
            return whois_server
        return None
      
    def fetch_domain_tld(self, domain):
        try:
            return get_tld(domain)
        except ValueError:
            return None
