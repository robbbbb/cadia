import pymysql.cursors
import os
from utils import clickhouse_conn
from datetime import datetime, date

# kimberly env password
kimberly_password = os.getenv("NETFLEET_KIMBERLEY_DB_PASS")


# Connect to the database kimberley
conn_kimberly_db=pymysql.connect(host="netfleet_kimberley-db",user="netfleet_kimberley",password=kimberly_password,
                                 database="netfleet_kimberley")
cur_kimberly_db = conn_kimberly_db.cursor()


# clickhouse connection
conn = clickhouse_conn()

# Execute the query select only whois_check_status = 'Not Available' and 'Available'
cur_kimberly_db.execute("""SELECT d.domain, d.whois_check_status, di.estimated_created_date FROM domains 
            AS d LEFT JOIN domains_info AS di ON d.id = di.domain_id WHERE d.whois_check_status 
            IN ('available', 'not available')""")

rows = cur_kimberly_db.fetchall()
# convert whois_check_status to available = False and not available = True
results = []

for row in rows:
    available = True if row[1] == 'Not Available' else False
    if row[2] is not None:
        registration_date = datetime(row[2].year, row[2].month, row[2].day, 0, 0, 0)
        # if the registration date is less than 1985-01-01,
        # because first domain was registered in 1985 below that date is invalid
        if registration_date  < datetime(1985, 1, 1, 0, 0, 0):
            registration_date = None
    else:
        registration_date = None
    results.append((row[0], available, registration_date))

batch_size = 5000

for row in range(0, len(results), batch_size):
    batch = results[row:row+batch_size]
    
    # check in click house if the domain already exist
    domains = [x[0] for x in batch]

    # Query ClickHouse to check for existing domains
    query = "SELECT domain FROM domains WHERE domain IN ('%s')" % "','".join(domains)

    # Get existing domains
    existing_domains = [x[0] for x in conn.execute(query)]

    # Filter out domains that already exist in the database
    batch = [x for x in batch if x[0] not in existing_domains]
    
    print("batch data", batch)


    
    # Insert the batch data to clickhouse
    conn.execute("INSERT INTO domains (domain, registered, registration_date) VALUES",
                    batch, types_check=True)
 
