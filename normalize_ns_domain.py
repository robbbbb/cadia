from utils import clickhouse_conn, normalize, check_mutations

# Establish connection
conn = clickhouse_conn()

# Query to get distinct ns_domains
sql = "SELECT DISTINCT ns_domains FROM domains"
rows = conn.execute(sql)
result = [dict(zip(['ns_domains'], row)) for row in rows]

# Loop through each domain to normalize ns_domains
for domain in result: # test domains
    ns_domains = sorted(list({normalize(n) for n in domain['ns_domains'] if normalize(n) is not None}))
    
    if ns_domains and ns_domains != domain['ns_domains']:
        query = f"""
        ALTER TABLE domains
        UPDATE ns_domains = {ns_domains}
        WHERE arraySort(ns_domains) = arraySort({domain['ns_domains']})
        """
        results = conn.execute(query)
        
        print(f"Updating ns_domains {domain['ns_domains']} to {ns_domains}") # logging
        
        check_mutations()
