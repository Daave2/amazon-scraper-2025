#!/usr/bin/env python3
"""Test specific failing SKU"""
import requests
import json

# Load config
with open('config.json') as f:
    config = json.load(f)

API_KEY = config['morrisons_api_key']
BEARER_TOKEN_URL = config.get('morrisons_bearer_token_url')

# Fetch bearer token
response = requests.get(BEARER_TOKEN_URL, timeout=10)
bearer_token = response.text.strip()

# Test the FAILING SKUs from the logs
failing_skus = ["114066665", "104056640", "108305383", "114224897"]

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {bearer_token}",
    "User-Agent": "Mozilla/5.0 (INF Scraper-StockChecker)"
}

for sku in failing_skus:
    product_url = f"https://api.morrisons.com/product/v1/items/{sku}?apikey={API_KEY}"
    
    print(f"\n{'='*60}")
    print(f"Testing SKU: {sku}")
    print(f"URL: {product_url}")
    
    try:
        resp = requests.get(product_url, headers=headers, timeout=15)
        print(f"Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            print("✅ SUCCESS!")
            data = resp.json()
            print(f"Product: {data.get('description', 'N/A')}")
        elif resp.status_code == 404:
            print("❌ Product not found (404) - This SKU doesn't exist in Morrisons system")
        elif resp.status_code == 401:
            print(f"❌ 401 Error")
            print(f"Response: {resp.text[:200]}")
        else:
            print(f"❌ Error {resp.status_code}")
            print(f"Response: {resp.text[:200]}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
