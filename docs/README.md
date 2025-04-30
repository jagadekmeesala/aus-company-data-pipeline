# Company Matching Pipeline

A Python-based data pipeline to perform fuzzy matching between company names scraped from websites and official records from the Australian Business Register (ABR), storing match results in a PostgreSQL database.

---

## Database Schema (PostgreSQL DDL)

```sql
-- Table: company_websites
CREATE TABLE company_websites (
    id SERIAL PRIMARY KEY,
    url VARCHAR(255) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    industry VARCHAR(255),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: abr_data
CREATE TABLE abr_data (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(11) UNIQUE NOT NULL,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(100),
    entity_status VARCHAR(50),
    state VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: company_match
CREATE TABLE company_match (
    id SERIAL PRIMARY KEY,
    website_id INT NOT NULL,
    abr_id INT NOT NULL,
    match_score DECIMAL(5,2),
    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (website_id) REFERENCES company_websites(id) ON DELETE CASCADE,
    FOREIGN KEY (abr_id) REFERENCES abr_data(id) ON DELETE CASCADE
);

-- Backup of ABR data (optional)
ALTER TABLE abr_data RENAME TO abr_data_backup;
```

### Entity Relationship Diagram
![image](https://github.com/user-attachments/assets/812602a5-a2d6-4f80-9736-49d014538636)

---

## Pipeline Design
```
+--------------------+         +-----------------+         +--------------------+
|  company_websites  |         |     abr_data    |         |  abr_data_backup   |
|  (PostgreSQL)      |         |  (PostgreSQL)   |         |  (PostgreSQL)      |
+--------------------+         +-----------------+         +--------------------+
         |                             |                             |
         |                             |                             |
         +-------------+---------------+                             |
                       |                                             |
                       v                                             v
             +---------------------+                         +--------------------+
             |   Python Script     |                         |   Validation Step  |
             | - pandas            |<------------------------| (Filter abr_id only |
             | - sqlalchemy        |                         |   in abr_data_backup)|
             | - rapidfuzz         |                         +--------------------+
             +---------------------+
                       |
                       v
             +----------------------+
             |  company_match Table |
             |   (Fuzzy Matches)    |
             +----------------------+
```

### Description:
- Data is loaded from company_websites and abr_data.
- Fuzzy matching is performed using RapidFuzz (token_sort_ratio) with a threshold.
- Matches are validated against a backup table (abr_data_backup) to ensure abr_id integrity.
- Valid matches are inserted into the company_match table.

---

## Technology Stack Justification
- Python	- Orchestrating the pipeline and data processing
- pandas	- Efficient in-memory data manipulation and filtering
- SQLAlchemy	- ORM and connection layer to interact with PostgreSQL
- RapidFuzz	- Fast and accurate fuzzy string matching with low memory overhead
- PostgreSQL	- Relational database to store scraped data and match results

---

## Setup & Running Instructions
1. Install Dependencies

``` pip install pandas sqlalchemy psycopg2-binary rapidfuzz ```

2. Configure Database
Make sure PostgreSQL is running and create a database called company_info. Update the database credentials in your script:
```
DB_USER = 'postgres'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'company_info'
```

3. Create Tables
Run the SQL in the Database Schema section to create all necessary tables in your PostgreSQL DB.

4. Load Your Data
Insert data into company_websites and abr_data.
Optionally, back up abr_data by renaming it to abr_data_backup before re-importing simplified data.

5. Run the Pipeline
``` python pipeline.py ```

The script will:
- Fetch data from PostgreSQL,
- Perform fuzzy matching,
- Validate abr_id against the backup,
- Insert valid matches into company_match.

## 2. Code Implementation

---

### DBT Transformation Logic 

```sql
-- models/cleaned_abr_data.sql
SELECT
  id,
  abn,
  UPPER(TRIM(entity_name)) AS entity_name_clean,
  entity_type,
  entity_status,
  state,
  extracted_at
FROM {{ source('public', 'abr_data') }}
WHERE abn IS NOT NULL;

-- models/cleaned_websites.sql
SELECT
  id,
  url,
  UPPER(TRIM(company_name)) AS company_name_clean,
  industry,
  extracted_at
FROM {{ source('public', 'company_websites') }}
WHERE company_name IS NOT NULL;

-- models/matched_companies.sql
SELECT
  cm.website_id,
  w.company_name_clean AS website_name,
  a.entity_name_clean AS matched_entity,
  cm.match_score,
  cm.matched_at
FROM {{ ref('company_match') }} cm
JOIN {{ ref('cleaned_abr_data') }} a ON cm.abr_id = a.id
JOIN {{ ref('cleaned_websites') }} w ON cm.website_id = w.id
WHERE cm.match_score > 80;



## Validated test cases

## Section 1: Data Read from PostgreSQL
### Test Case 1.1: Validate Data Read from company_websites
Objective: Ensure company_df loads valid data from company_websites table.

Steps:
Connect to the PostgreSQL database.
Run the query: SELECT id, company_name FROM company_websites.

Expected Result: A DataFrame with non-null id and company_name values.

### Test Case 1.2: Validate Data Read from abr_data
Objective: Ensure abr_df loads valid data from abr_data table.

Steps:
Connect to the database.
Run the query: SELECT id, entity_name FROM abr_data.

Expected Result: A DataFrame with non-null id and entity_name.

## Section 2: Fuzzy Matching Logic
### Test Case 2.1: Match Threshold Logic
Objective: Confirm that only matches with score ≥ 75 are included.

Steps:
Set threshold = 75.
Match company_name with entity_name using fuzz.token_sort_ratio.

Expected Result: No entries in the match list have a score < 75.

### Test Case 2.2: Unique Match Validation
Objective: Confirm that each company matches to at most one ABR entity.

Steps:
Use process.extractOne() for each company.

Expected Result: One match (or none) per company record.

## Section 3: Data Insertion to company_match
Test Case 3.1: Valid Foreign Key Reference
Objective: Ensure all abr_id and website_id exist in their respective tables.

Steps:
Attempt insert into company_match.

Expected Result: No foreign key violations.

### Test Case 3.2: Data Type Validation
Objective: Ensure proper data types are used for match_score, matched_at, etc.

Steps:
Check DataFrame types before insertion.

Expected Result: match_score is numeric (decimal), matched_at is timestamp.

### Test Case 3.3: Insert Failure Handling
Objective: Confirm code catches and logs insertion errors.

Steps:
Introduce an intentional duplicate or FK violation.
Run the script.

Expected Result: Error is printed, but script does not crash.
