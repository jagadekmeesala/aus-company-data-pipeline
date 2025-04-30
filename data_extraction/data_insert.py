import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# PostgreSQL connection settings
db_config = {
    'dbname': 'company_info',
    'user': 'postgres',
    'password': 'root',
    'host': 'localhost',
    'port': 5432
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# ----------------- Load CommonCrawl Data -----------------
print("Inserting CommonCrawl data...")
cc_df = pd.read_csv('cc_australian_companies.csv')

# Rename columns to match DB schema if needed
cc_df.rename(columns={
    'Website URL': 'url',
    'Company Name': 'company_name',
    'Industry': 'industry'
}, inplace=True)

# Prepare list of tuples for insertion
cc_records = cc_df[['url', 'company_name', 'industry']].values.tolist()

# Insert into company_websites
insert_cc_query = """
INSERT INTO company_websites (url, company_name, industry)
VALUES %s
ON CONFLICT (url) DO NOTHING
"""
execute_values(cursor, insert_cc_query, cc_records)
print(f"Inserted {cursor.rowcount} rows into company_websites.")

# ----------------- Load ABR Data -----------------
print("Inserting ABR data...")
abr_df = pd.read_csv('abr_extract_output.csv')

# Rename columns to match DB schema
abr_df.rename(columns={
    'ABN': 'abn',
    'Entity Name': 'entity_name',
    'Entity Type': 'entity_type',
    'GST Status': 'entity_status',
    'State': 'state'
}, inplace=True)

# Prepare list of tuples
abr_records = abr_df[['abn', 'entity_name', 'entity_type', 'entity_status', 'state']].values.tolist()

# Insert into abr_data
insert_abr_query = """
INSERT INTO abr_data (abn, entity_name, entity_type, entity_status, state)
VALUES %s
ON CONFLICT (abn) DO NOTHING
"""
execute_values(cursor, insert_abr_query, abr_records)
print(f"Inserted {cursor.rowcount} rows into abr_data.")

# Commit & close
conn.commit()
cursor.close()
conn.close()

print("Data insertion complete.")
