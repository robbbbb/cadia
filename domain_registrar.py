from utils import postgres_conn, clickhouse_conn, check_mutations
import os
import csv

"""
This script insert the registrar names and registrar_id in the 'domain_registrar' table of a Postgres database.
It reads registrar data from two CSV files ('registrar-ids.csv' and 'registrar-ids-au.csv'),
merges the data, and insert the 'registrar_name', and 'registrar_id' field in the database where necessary.
Then update the registrar name in cadia database base on registrar_id
"""

csv_data = []

# get the path for registrar-ids.csv and registrar-ids-au.csv
script_dir = os.path.dirname(os.path.abspath(__file__))
rel_path = "data/registrar-ids.csv"
rel_path_au = "data/registrar-ids-au.csv"
abs_file_path = os.path.join(script_dir, rel_path)
abs_file_path_au = os.path.join(script_dir, rel_path_au)

with open(abs_file_path_au, mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        csv_data.append({
            'registrar_id': row['registrar_id'],
            'registrar_name': (row['registrar_name']).lower()
        })


with open(abs_file_path, mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        csv_data.append({
            'registrar_id': row['id'],
            'registrar_name': row['registrar_name']
        })

        
# make it a dict to make no duplicates
data = {row['registrar_id']: row['registrar_name'] for row in csv_data}



pg = postgres_conn()
cursor = pg.cursor()
print("Inserting/Updating registrar_id and registrar_name in PostgreSQL")

domain_registrar_updated = []

# Insert/update in PostgreSQL
for row in data.items():
    cursor.execute("""
        INSERT INTO public.domain_registrar (registrar_id, registrar_name)
        VALUES (%s, %s)
        ON CONFLICT (registrar_id)
        DO UPDATE SET registrar_name = EXCLUDED.registrar_name
        WHERE domain_registrar.registrar_name IS DISTINCT FROM EXCLUDED.registrar_name
        RETURNING registrar_id, registrar_name
    """, row)

    result = cursor.fetchall()
    if result:
        print(f"Registrar name that is new or have update in Postgres: {result}")
        domain_registrar_updated.extend(result)

pg.commit()

print("Done inserting/updating PostgreSQL.")
print("Rows updated:", domain_registrar_updated)

# Prepare ClickHouse update
if domain_registrar_updated:
    conn = clickhouse_conn()

    # Ensure format is list of tuples
    data = [(row[0], row[1]) for row in domain_registrar_updated]

    for registrar_id, registrar_name in data:
        print(f"Updating ClickHouse domain with registrar_id: {registrar_id}")
        update_query = """
        ALTER TABLE domains
        UPDATE registrar_name = %(registrar_name)s
        WHERE registrar_id = %(registrar_id)s
        """
        conn.execute(update_query, {
            'registrar_name': registrar_name,
            'registrar_id': registrar_id
        })
        check_mutations()

else:
    print("No changes to propagate to ClickHouse.")