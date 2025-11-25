import json

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

# Check Morrisons config
print("=== Morrisons API Configuration ===")
print(f"API Key: {'SET' if config.get('morrisons_api_key') else 'NOT SET'}")
print(f"Bearer Token URL: {config.get('morrisons_bearer_token_url', 'NOT SET')}")
print(f"Enrich Stock Data: {config.get('enrich_stock_data', False)}")

# Test fetching bearer token
if config.get('morrisons_bearer_token_url'):
    from stock_enrichment import fetch_bearer_token_from_gist
    token = fetch_bearer_token_from_gist(config.get('morrisons_bearer_token_url'))
    print(f"\nBearer Token Fetch: {'SUCCESS' if token else 'FAILED'}")
    if token:
        print(f"Token Preview: {token[:20]}..." if len(token) > 20 else token)
