from attr import asdict, attrs, attrib, fields_dict, validators
from datetime import date, datetime
from dateutil.parser import isoparse
from typing import List
from utils import json_serial
import json

def datetime_or_iso(dt):
    if dt is None:
        return None
    if type(dt) == datetime:
        return dt
    return isoparse(dt)

def date_or_iso(d):
    if type(d) == date:
        return d
    return date.fromisoformat(d)

def lower(s):
    return s.lower()

def list_lower(l):
    return [ s.lower() for s in l ]

@attrs
class Result():
    domain: str = attrib(validator=validators.instance_of(str), converter=lower)
    modified_by: str = attrib(validator=validators.instance_of(str))

    def asdict(self):
        d = asdict(self)
        d['type'] = self.__class__.__name__ # Add class name in 'type' key
        return d

    def serialize(self):
        return json.dumps(self.asdict(), default=json_serial)

    # Given dict, make and return appropriate result object
    # Every new result subclass needs to be added here
    @classmethod
    def fromdict(cls,d):
        type = d['type']
        del(d['type'])
        return {
                'Result' : Result,
                'ZonefileResult' : ZonefileResult,
                'DNSResult' : DNSResult,
                'PIResult' : PIResult,
                'RDAPResult' : RDAPResult,
                'RDAPNXResult' : RDAPNXResult,
                'AbovePMResult' : AbovePMResult,
                'WHOISResult' : WHOISResult,
                'WHOISNXResult' : WHOISNXResult
        }[type](**d)
    
    @classmethod
    def deserialize(cls,j):
        return cls.fromdict(json.loads(j))

    @classmethod
    def cols(cls):
        return [a.name for a in cls.__attrs_attrs__]

@attrs
class DNSResult(Result):
    addr: List[str] = attrib(validator=validators.instance_of(list))
    ns: List[str] = attrib(validator=validators.instance_of(list),converter=list_lower)
    txt: List[str] = attrib(validator=validators.instance_of(list))
    mx: List[str] = attrib(validator=validators.instance_of(list),converter=list_lower)
    dns_exists: bool = attrib(validator=validators.instance_of(bool))
    rcode: str = attrib(validator=validators.instance_of(str))
    ns_domains: List[str] = attrib(validator=validators.instance_of(list),converter=list_lower)
    dns_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

    modified_by: str = attrib(default='dns', validator=validators.instance_of(str))
    # TODO last_modified must come last because it's optional. any way to not repeat it in each subclass?
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class ZonefileResult(Result):
    ns: List[str] = attrib(validator=validators.instance_of(list),converter=list_lower)
    ns_domains: List[str] = attrib(validator=validators.instance_of(list),converter=list_lower)
    tld: str = attrib(validator=validators.instance_of(str),converter=lower)
    zonefile_date: date = attrib(default=date.today(), validator=validators.instance_of(date), converter=date_or_iso)
    modified_by: str = attrib(default='zonefile', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class RDAPResult(Result):
    rdap_json: str = attrib(validator=validators.instance_of(str))
    registered: bool = attrib(validator=validators.instance_of(bool))
    registration_date: datetime = attrib(validator=validators.instance_of(datetime), converter=datetime_or_iso)
    expiration_date: datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso)
    transfer_date: datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso)
    last_changed_date: datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso)
    
    registrant_name: str = attrib(validator=validators.optional(validators.instance_of(str)))
    registrant_organization: str = attrib(validator=validators.optional(validators.instance_of(str)))
    registrant_adr: List[str] = attrib(validator=validators.instance_of(list))
    registrant_state: str = attrib(validator=validators.optional(validators.instance_of(str)))
    registrant_country: str = attrib(validator=validators.optional(validators.instance_of(str)))
    registrant_phone: str = attrib(validator=validators.optional(validators.instance_of(str)))
    registrant_email: str = attrib(validator=validators.optional(validators.instance_of(str)))


    tech_name: str = attrib(validator=validators.optional(validators.instance_of(str)))
    tech_organization: str = attrib(validator=validators.optional(validators.instance_of(str)))
    tech_adr: List[str] = attrib(validator=validators.instance_of(list))
    tech_state: str = attrib(validator=validators.optional(validators.instance_of(str)))
    tech_country: str = attrib(validator=validators.optional(validators.instance_of(str)))
    tech_phone: str = attrib(validator=validators.optional(validators.instance_of(str)))
    tech_email: str = attrib(validator=validators.optional(validators.instance_of(str)))

   
    admin_name: str = attrib(validator=validators.optional(validators.instance_of(str)))
    admin_organization: str = attrib(validator=validators.optional(validators.instance_of(str)))
    admin_adr: List[str] = attrib(validator=validators.instance_of(list))
    admin_state: str = attrib(validator=validators.optional(validators.instance_of(str)))
    admin_country: str = attrib(validator=validators.optional(validators.instance_of(str)))
    admin_phone: str = attrib(validator=validators.optional(validators.instance_of(str)))
    admin_email: str = attrib(validator=validators.optional(validators.instance_of(str)))

   
    registrar_id: int = attrib(validator=validators.optional(validators.instance_of(int)))
    
    status: List[str] = attrib(validator=validators.instance_of(list))

    rdap_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    whois_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    modified_by: str = attrib(default='rdap', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class RDAPNXResult(Result):
    # RDAP result for a domain that does not exist. To avoid overwriting exp date etc that may be needed
    registered: bool = attrib(validator=validators.instance_of(bool))

    # TODO should set registrar id to null here? any other fields?
    rdap_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    whois_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    modified_by: str = attrib(default='rdap', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class PIResult(Result):
    # Traffic data from PI
    visits_12m: int = attrib(validator=validators.instance_of(int), converter=int)
    hits_12m: int = attrib(validator=validators.instance_of(int), converter=int)
    visits_arr_12m: List[int] = attrib(validator=validators.instance_of(list))
    last_visit_date: date = attrib(validator=validators.instance_of(date), converter=date_or_iso)
    hits_geo: str = attrib(validator=validators.instance_of(str))
    pi_rank: int = attrib(validator=validators.instance_of(int), converter=int)
    pi_import_date: date = attrib(default=date.today(), validator=validators.instance_of(date), converter=date_or_iso)

    modified_by: str = attrib(default='pi', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class WHOISResult(Result):
    whois_text: str = attrib(validator=validators.instance_of(str))
    registered: bool = attrib(validator=validators.instance_of(bool))

    #updated_date: datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso)
    last_changed_date: datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso)
   
    registrar_name: str = attrib(validator=validators.optional(validators.instance_of(str)))
    registrar_id: int = attrib(validator=validators.instance_of(int), converter=int)
   
    status: List[str] = attrib(validator=validators.instance_of(list))

    # Optional args below - must be after all mandatory ones
    expiration_date: datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso, default=None)
    registration_date:datetime = attrib(validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso, default=None)

    registrant_name: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    registrant_organization: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    registrant_stateprovince: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    registrant_country: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    registrant_phone: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    registrant_email: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
   
    tech_name: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    tech_organization: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    tech_stateprovince: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    tech_country: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    tech_phone: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    tech_email: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
   
    admin_name: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    admin_organization: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    admin_stateprovince: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    admin_country: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    admin_phone: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    admin_email: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)

    # Registrant (different from registrant contact) and ID (eg ABN) for au
    registrant: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    registrant_id: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)

    # Eligibility type for au
    eligibility_type: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    eligibility_name: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)
    eligibility_id: str = attrib(validator=validators.optional(validators.instance_of(str)), default=None)

    whois_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    modified_by: str = attrib(default='whois', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class WHOISNXResult(Result):
    registered: bool = attrib(validator=validators.instance_of(bool))   
    
    whois_scraped: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    modified_by: str = attrib(default='whois', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

@attrs
class AbovePMResult(Result):
    pm_username: str = attrib(validator=validators.optional(validators.instance_of(str)))
    pm_blacklisted: datetime = attrib(factory=datetime.now, validator=validators.optional(validators.instance_of(datetime)), converter=datetime_or_iso)
    pm_added: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)
    
    modified_by: str = attrib(default='above_pm', validator=validators.instance_of(str))
    last_modified: datetime = attrib(factory=datetime.now, validator=validators.instance_of(datetime), converter=datetime_or_iso)

# NOTE: if you add a new Result subclass, also add it in Result.fromdict()
