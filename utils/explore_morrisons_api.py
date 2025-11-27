"""
Script to explore Morrisons API responses by dumping full JSON data.
Useful for identifying new fields to scrape.
"""
import json
import requests
import sys

# Load config
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print("Error: config.json not found.")
    sys.exit(1)

API_KEY = config.get('morrisons_api_key')
BEARER_TOKEN_URL = config.get('morrisons_bearer_token_url')

if not API_KEY:
    print("Error: morrisons_api_key not found in config.json")
    sys.exit(1)

# Fetch bearer token
bearer_token = None
if BEARER_TOKEN_URL:
    try:
        response = requests.get(BEARER_TOKEN_URL, timeout=10)
        response.raise_for_status()
        bearer_token = response.text.strip()
        print(f"✓ Bearer token fetched")
    except Exception as e:
        print(f"✗ Failed to fetch bearer token: {e}")

# Test parameters (using the ones from test script)
TEST_SKU = "112571916" 
TEST_LOCATION = "3828"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (INF Scraper-Explorer)",
}
if bearer_token:
    HEADERS["Authorization"] = f"Bearer {bearer_token}"

def fetch_and_dump(name, url):
    print(f"\n{'='*20} {name} API {'='*20}")
    print(f"URL: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            print(json.dumps(data, indent=2))
        else:
            print(f"Error Response: {r.text}")
    except Exception as e:
        print(f"Request failed: {e}")

# 1. Product API
product_url = f"https://api.morrisons.com/product/v1/items/{TEST_SKU}?apikey={API_KEY}"
fetch_and_dump("PRODUCT", product_url)

# 2. Stock API
stock_url = f"https://api.morrisons.com/stock/v2/locations/{TEST_LOCATION}/items/{TEST_SKU}?apikey={API_KEY}"
fetch_and_dump("STOCK", stock_url)

# 3. Location (Price Integrity) API
locn_url = f"https://api.morrisons.com/priceintegrity/v1/locations/{TEST_LOCATION}/items/{TEST_SKU}?apikey={API_KEY}"
fetch_and_dump("LOCATION", locn_url)
