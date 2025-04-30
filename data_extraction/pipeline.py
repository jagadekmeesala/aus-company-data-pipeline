import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz
from datetime import datetime
import time

# ---------- CONFIGURATION ----------
DB_USER = 'postgres'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'company_info'

# SQLAlchemy engine
db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)
# ----------------------------------

print("Reading data from PostgreSQL...")

# Read the data from company_websites and abr_data tables
company_df = pd.read_sql("SELECT id, company_name FROM company_websites", engine)
abr_df = pd.read_sql("SELECT id, entity_name FROM abr_data", engine)

# Convert the abr_data into a list and a lookup dictionary for fast matching
abr_choices = abr_df['entity_name'].tolist()
abr_lookup = dict(zip(abr_df['entity_name'], abr_df['id']))

matches = []
threshold = 75  # Minimum score to accept a match

# Total number of companies to match
total = len(company_df)
progress_interval = max(total // 100, 1)  # Show progress every 1%

print(f"Starting fuzzy matching for {total} companies...")
start = time.time()

# Iterate over the company dataframe and perform fuzzy matching
for idx, row in enumerate(company_df.itertuples(index=False)):
    best_match = process.extractOne(
        row.company_name,
        abr_choices,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold
    )

    if best_match:
        matched_name, score, _ = best_match
        abr_id = abr_lookup.get(matched_name)  # Safe lookup with .get()

        # Only add match if valid abr_id is found
        if abr_id:  
            matches.append({
                'website_id': row.id,
                'abr_id': abr_id,
                'match_score': score,
                'matched_at': datetime.utcnow()  # Use UTC to avoid timezone issues
            })

    # Progress report every progress_interval or last iteration
    if (idx + 1) % progress_interval == 0 or (idx + 1) == total:
        print(f"Progress: {(idx + 1) / total * 100:.1f}%")

print(f"Matching completed in {time.time() - start:.2f} seconds.")

# Batch insert matches into the 'company_match' table if there are valid matches
if matches:
    print(f"Inserting {len(matches)} matches into 'company_match'...")
    match_df = pd.DataFrame(matches)

    try:
        match_df.to_sql('company_match', engine, if_exists='append', index=False, method='multi')
        print("Data inserted successfully.")
    except Exception as e:
        print(f"❌ Error inserting into DB: {e}")
else:
    print("No valid matches found.")
