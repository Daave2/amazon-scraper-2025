#!/usr/bin/env python3
"""Quick test script to debug Morrisons API authentication"""
import requests
import json

# Load config
with open('config.json') as f:
    config = json.load(f)

API_KEY = config['morrisons_api_key']
BEARER_TOKEN_URL = config.get('morrisons_bearer_token_url')

# Fetch bearer token
print(f"Fetching bearer token from: {BEARER_TOKEN_URL}")
response = requests.get(BEARER_TOKEN_URL, timeout=10)
bearer_token = response.text.strip()
print(f"Bearer token fetched (length: {len(bearer_token)})")
print(f"First 20 chars: {bearer_token[:20]}...")
print(f"Last 3 chars: ...{bearer_token[-3:]}")
print(f"Has whitespace: {bearer_token != bearer_token.strip()}")
print()

# Test SKU (from your logs)
test_sku = "111375690"
test_location = "218"  # Acton

# Test Product API
product_url = f"https://api.morrisons.com/product/v1/items/{test_sku}?apikey={API_KEY}"
headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {bearer_token}",
    "User-Agent": "Mozilla/5.0 (INF Scraper-StockChecker)"
}

print(f"Testing Product API for SKU {test_sku}...")
print(f"URL: {product_url}")
print(f"Headers: {headers}")
print()

try:
    resp = requests.get(product_url, headers=headers, timeout=15)
    print(f"Status Code: {resp.status_code}")
    print(f"Response Headers: {dict(resp.headers)}")
    print(f"Response Body: {resp.text[:500]}")
    
    if resp.status_code == 200:
        print("\n✅ SUCCESS! API authentication is working correctly.")
    else:
        print(f"\n❌ FAILED with status {resp.status_code}")
        print(f"Full response: {resp.text}")
        
except Exception as e:
    print(f"❌ ERROR: {e}")
