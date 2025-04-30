import requests
import gzip
import json
import re
import csv
from smart_open import open as smart_open
from warcio.archiveiterator import ArchiveIterator

MAX_RESULTS = 2500  
visited_domains = set()
output_data = []

valid_au_domains = [
    ".com.au", ".org.au", ".net.au", ".gov.au", ".edu.au", ".id.au", ".asn.au"
]

def safe_get(dictionary, keys, default=""):
    """Safely get a nested key in a dictionary."""
    for key in keys:
        dictionary = dictionary.get(key, {})
    return dictionary if dictionary else default

def extract_root_domain(url):
    """Extract and clean root domain from URL, only if it ends with valid .au domain."""
    if not url.startswith("http"):
        return ""

    domain = url.replace("https://", "").replace("http://", "").split("/")[0]

    if domain.startswith(("www.", "web.", "www1.")):
        domain = domain.split(".", 1)[1]

    if not any(domain.endswith(valid) for valid in valid_au_domains):
        return ""

    if ".au" not in domain:
        return ""

    return domain

def clean_company_name(domain):
    """Extract a cleaner Company Name from domain."""
    domain = domain.lower()

    suffixes_to_remove = [
        ".com.au", ".org.au", ".net.au", ".gov.au", ".edu.au",
        ".id.au", ".asn.au",
        ".com", ".org", ".net", ".gov", ".edu", ".au"
    ]

    for suffix in suffixes_to_remove:
        if domain.endswith(suffix):
            domain = domain[:-len(suffix)]

    domain = domain.rstrip(".")
    domain = domain.replace("-", " ")
    return domain.title().strip()

def guess_industry_from_metadata(metadata, domain):
    """Guess industry using metadata or domain keywords."""
    description = safe_get(metadata, ["Envelope", "Payload-Metadata", "HTTP-Response-Metadata", "HTML-Metadata", "Head", "Meta", "description"], "")
    keywords = safe_get(metadata, ["Envelope", "Payload-Metadata", "HTTP-Response-Metadata", "HTML-Metadata", "Head", "Meta", "keywords"], "")

    combined_text = (description + " " + keywords + " " + domain).lower()

    if any(word in combined_text for word in ["school", "university", "college", "education", "academy"]):
        return "Education"
    elif any(word in combined_text for word in ["bank", "finance", "insurance", "loan", "mortgage", "fund"]):
        return "Finance"
    elif any(word in combined_text for word in ["shop", "furniture", "wood", "store", "supermarket", "grocery", "retail", "mart", "market", "boutique"]):
        return "Retail"
    elif any(word in combined_text for word in ["hospital", "clinic", "medical", "healthcare", "pharmacy"]):
        return "Healthcare"
    elif any(word in combined_text for word in ["law", "legal", "lawyer", "attorney"]):
        return "Legal Services"
    elif any(word in combined_text for word in ["gov", "government", ".gov.au"]):
        return "Government"
    elif any(word in combined_text for word in ["it", "software", "tech", "technology", "ai", "cloud"]):
        return "Technology"
    elif any(word in combined_text for word in ["media", "marketing", "pr", "design", "promotion", "leads"]):
        return "Media and Marketing"
    else:
        return "Other"

def process_wat_file(wat_url):
    """Process a single WAT file and extract Australian company data."""
    global output_data

    try:
        with smart_open(wat_url, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type != 'metadata':
                    continue

                payload = record.content_stream().read()
                metadata = json.loads(payload)

                url = safe_get(metadata, ["Envelope", "WARC-Header-Metadata", "WARC-Target-URI"], "")
                if not url:
                    continue

                domain = extract_root_domain(url)
                if not domain:
                    continue

                if domain in visited_domains:
                    continue
                visited_domains.add(domain)

                company_name = safe_get(metadata, ["Envelope", "Payload-Metadata", "HTTP-Response-Metadata", "HTML-Metadata", "Title"], "")
                if not company_name:
                    company_name = clean_company_name(domain)

                industry = guess_industry_from_metadata(metadata, domain)

                output_data.append({
                    "Website URL": url,
                    "Company Name": company_name,
                    "Industry": industry
                })

                if len(output_data) >= MAX_RESULTS:
                    print(f"Collected {MAX_RESULTS} results. Stopping.")
                    return

    except Exception as e:
        print(f"Error processing {wat_url}: {e}")

def download_and_process_wat_files():
    """Main driver function."""
    url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/wat.paths.gz'
    print("Downloading wat.paths.gz...")
    response = requests.get(url)
    if response.status_code == 200:
        with open('wat.paths.gz', 'wb') as f:
            f.write(response.content)
        print("Downloaded and saved 'wat.paths.gz'.")
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        return

    print("Unzipping 'wat.paths.gz'...")
    with gzip.open('wat.paths.gz', 'rb') as f_in:
        with open('wat.paths', 'wb') as f_out:
            f_out.write(f_in.read())
    print("Unzipped to 'wat.paths'.")

    print("Reading WAT file paths...")
    wat_files = []
    with open('wat.paths', 'r') as f:
        for line in f:
            wat_files.append('https://data.commoncrawl.org/' + line.strip())

    print(f"Total WAT files available: {len(wat_files)}")

    for wat_url in wat_files:
        print(f"Processing {wat_url}...")
        process_wat_file(wat_url)
        if len(output_data) >= MAX_RESULTS:
            break

    with open('cc_australian_companies.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Website URL", "Company Name", "Industry"])
        writer.writeheader()
        writer.writerows(output_data)
    print(f"Saved {len(output_data)} records to 'cc_australian_companies.csv'.")

if __name__ == "__main__":
    download_and_process_wat_files()
