"""
Test script to verify Morrisons API authentication and endpoints.
"""
import json
import requests

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

API_KEY = config.get('morrisons_api_key')
BEARER_TOKEN_URL = config.get('morrisons_bearer_token_url')

print("=" * 60)
print("MORRISONS API DIAGNOSTIC TEST")
print("=" * 60)

# Step 1: Check if credentials are configured
print("\n1. Configuration Check:")
print(f"   API Key: {'✓ SET' if API_KEY else '✗ NOT SET'}")
print(f"   Bearer Token URL: {'✓ SET' if BEARER_TOKEN_URL else '✗ NOT SET'}")

if not API_KEY:
    print("\n❌ ERROR: API Key is not configured!")
    exit(1)

# Step 2: Fetch bearer token
print("\n2. Fetching Bearer Token:")
bearer_token = None
if BEARER_TOKEN_URL:
    try:
        response = requests.get(BEARER_TOKEN_URL, timeout=10)
        response.raise_for_status()
        bearer_token = response.text.strip()
        print(f"   ✓ Token fetched successfully")
        print(f"   Token preview: {bearer_token[:15]}..." if len(bearer_token) > 15 else f"   Token: {bearer_token}")
    except Exception as e:
        print(f"   ✗ Failed to fetch token: {e}")
else:
    print("   ⚠ Bearer Token URL not configured - trying with API key only")

# Step 3: Test API endpoints
test_sku = "112571916"  # From your error logs
test_location = "3828"  # Example Morrisons store number

print(f"\n3. Testing API Endpoints (SKU: {test_sku}, Location: {test_location}):")

# Test 1: Product API (with bearer token)
print("\n   a) Product API (with bearer token):")
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (INF Scraper-StockChecker)",
}
if bearer_token:
    headers["Authorization"] = f"Bearer {bearer_token}"

product_url = f"https://api.morrisons.com/product/v1/items/{test_sku}?apikey={API_KEY}"
try:
    r = requests.get(product_url, headers=headers, timeout=15)
    print(f"      Status: {r.status_code}")
    if r.status_code == 200:
        print(f"      ✓ SUCCESS - Product data retrieved")
        data = r.json()
        print(f"      Product: {data.get('description', 'N/A')}")
        
        # Check for image URL
        images = data.get("imageUrl", [])
        if images and isinstance(images, list) and len(images) > 0:
            print(f"      Image URL: {images[0].get('url', 'N/A')}")
    elif r.status_code == 401:
        print(f"      ✗ UNAUTHORIZED (401)")
        print(f"      Response: {r.text[:200]}")
    elif r.status_code == 403:
        print(f"      ✗ FORBIDDEN (403)")
        print(f"      Response: {r.text[:200]}")
    else:
        print(f"      ✗ ERROR: {r.status_code}")
        print(f"      Response: {r.text[:200]}")
except Exception as e:
    print(f"      ✗ Request failed: {e}")

# Test 2: Product API (without bearer token)
print("\n   b) Product API (without bearer token - API key only):")
headers_no_bearer = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (INF Scraper-StockChecker)",
}
try:
    r = requests.get(product_url, headers=headers_no_bearer, timeout=15)
    print(f"      Status: {r.status_code}")
    if r.status_code == 200:
        print(f"      ✓ SUCCESS - Bearer token not needed!")
        data = r.json()
        print(f"      Product: {data.get('description', 'N/A')}")
    elif r.status_code == 401:
        print(f"      ✗ UNAUTHORIZED - API key or bearer token invalid")
    else:
        print(f"      ✗ ERROR: {r.status_code}")
except Exception as e:
    print(f"      ✗ Request failed: {e}")

# Test 3: Stock API
print(f"\n   c) Stock API (Location: {test_location}):")
stock_url = f"https://api.morrisons.com/stock/v2/locations/{test_location}/items/{test_sku}?apikey={API_KEY}"
if bearer_token:
    headers["Authorization"] = f"Bearer {bearer_token}"
try:
    r = requests.get(stock_url, headers=headers, timeout=15)
    print(f"      Status: {r.status_code}")
    if r.status_code == 200:
        print(f"      ✓ SUCCESS - Stock data retrieved")
        data = r.json()
        stock_data = data.get('stockPosition', [{}])[0]
        print(f"      Stock: {stock_data.get('qty', 'N/A')} {stock_data.get('unitofMeasure', '')}")
    elif r.status_code == 401:
        print(f"      ✗ UNAUTHORIZED (401)")
    elif r.status_code == 404:
        print(f"      ⚠ NOT FOUND (404) - SKU may not exist at this location")
    else:
        print(f"      ✗ ERROR: {r.status_code}")
except Exception as e:
    print(f"      ✗ Request failed: {e}")

# Recommendations
print("\n" + "=" * 60)
print("DIAGNOSIS:")
print("=" * 60)
print("""
If you see 401 UNAUTHORIZED errors above, the issue is likely:

1. **Expired Bearer Token**: Morrisons bearer tokens typically expire.
   - You need to refresh the token in your gist periodically
   - Or implement automatic token refresh

2. **Invalid API Key**: The API key may be incorrect or expired
   - Check with Morrisons API documentation
   - Verify your API key is still active

3. **Authentication Method**: Some APIs only need the API key OR bearer token
   - Try disabling bearer token if API key alone works

NEXT STEPS:
- If API key alone works, you can disable bearer token authentication
- If bearer token is needed, set up automatic refresh mechanism
- Contact Morrisons API support to verify your credentials
""")
