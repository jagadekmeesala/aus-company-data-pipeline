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


## Pipeline Design
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
