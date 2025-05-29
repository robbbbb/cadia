from utils import clickhouse_conn, tld
import time

conn = clickhouse_conn()

incorrect_tlds = []
BATCH_SIZE = 100_000
total_inserted = 0
MAX_WAIT_TIME = 600
WAIT_INTERVAL = 5
last_domain = ""
batch_number = 0
processed_count = 0

def insert_batch(batch, conn):
    insert_data = [
        (
            record["domain"] or "",
            record["in_db_tld"] or "",
            record["python_tld_method"] or ""
        )
        for record in batch
    ]
    print(f"Inserting batch of {len(insert_data)} records...")
    conn.execute("INSERT INTO cadia.tmp_incorrect_domain_tld VALUES", insert_data)

while True:
    print(f"\nProcessing batch #{batch_number} starting after domain: '{last_domain}'")
    rows = conn.execute(f"""
        SELECT domain, tld
        FROM domains
        WHERE domain > '{last_domain}'
        ORDER BY domain
        LIMIT {BATCH_SIZE}
    """)

    if not rows:
        print("No more rows to process.")
        break

    for domain, expected_tld in rows:
        if not domain:
            continue

        try:
            actual_tld = tld(domain)
        except Exception as e:
            print(f"Skipping domain with error: {domain} -> {e}")
            actual_tld = None

        if actual_tld != expected_tld and expected_tld != '' and actual_tld is not None:
            incorrect_tlds.append({
                "domain": domain,
                "in_db_tld": expected_tld,
                "python_tld_method": actual_tld
            })
            print(f"[Mismatch] Domain: {domain}, DB TLD: {expected_tld}, Python TLD: {actual_tld}")

    if incorrect_tlds:
        insert_batch(incorrect_tlds, conn)
        total_inserted += len(incorrect_tlds)
        incorrect_tlds.clear()

    last_domain = rows[-1][0]
    processed_count += len(rows)
    print(f"Total rows processed: {processed_count}")
    batch_number += 1

if incorrect_tlds:
    insert_batch(incorrect_tlds, conn)
    total_inserted += len(incorrect_tlds)
    incorrect_tlds.clear()

''' Just insert all the incorrect tlds in the tmp table
# Enable nondeterministic mutations
conn.execute("SET allow_nondeterministic_mutations = 1")

print(f"\nTotal incorrect TLDs found: {total_inserted}")
print("Running bulk UPDATE mutation...")

conn.execute("""
    ALTER TABLE cadia.domains
    UPDATE tld = tmp.python_tld_method
    FROM cadia.tmp_incorrect_domain_tld AS tmp
    WHERE domains.domain = tmp.domain
    """)

print("Waiting for mutation to complete...")
start_time = time.time()
while time.time() - start_time < MAX_WAIT_TIME:
    mutation_in_progress = conn.execute("""
        SELECT 1 FROM system.mutations
        WHERE NOT is_done AND table = 'domains' AND database = 'cadia'
    """)
    if mutation_in_progress:
        print(f"Mutation in progress... waiting {WAIT_INTERVAL}s")
        time.sleep(WAIT_INTERVAL)
    else:
        print("Mutation completed.")
        break
else:
    raise TimeoutError("Mutation did not finish within expected time.")

# Uncomment this when you're ready to clear the temp table:
# print("Truncating temporary table...")
# conn.execute("TRUNCATE TABLE cadia.tmp_incorrect_domain_tld")
# time.sleep(WAIT_INTERVAL)
'''