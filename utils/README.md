# Diagnostic Utilities

This directory contains utility scripts for testing and troubleshooting the Amazon Seller Central Scraper.

## Available Scripts

### `test_morrisons_api.py`

**Purpose**: Comprehensive diagnostic tool for Morrisons API authentication and connectivity.

**Usage**:
```bash
python3 utils/test_morrisons_api.py
```

**What it tests**:
- ✅ Configuration validation (API key and bearer token URL)
- ✅ Bearer token fetch from GitHub Gist
- ✅ Product API endpoint (with and without bearer token)
- ✅ Stock API endpoint
- ✅ Authentication method compatibility

**Output**:
- Status codes for each API call
- Success/failure indicators
- Diagnostic recommendations

**When to use**:
- Getting 401 Unauthorized errors from Morrisons API
- Verifying API credentials are correct
- Troubleshooting bearer token issues
- Checking API endpoint availability

---

### `test_morrisons_config.py`

**Purpose**: Quick configuration checker for Morrisons API settings.

**Usage**:
```bash
python3 utils/test_morrisons_config.py
```

**What it checks**:
- ✅ API key is configured
- ✅ Bearer token URL is configured
- ✅ Stock enrichment is enabled
- ✅ Bearer token can be fetched successfully

**Output**:
```
=== Morrisons API Configuration ===
API Key: SET
Bearer Token URL: https://gist.githubusercontent.com/...
Enrich Stock Data: True

Bearer Token Fetch: SUCCESS
Token Preview: pBkGWHfOaC52zNC...
```

**When to use**:
- Quick validation after updating config.json
- Verifying Morrisons API settings before running scraper
- Checking if bearer token Gist is accessible

---

## Common Issues & Solutions

### Issue: 401 Unauthorized Errors

**Symptoms**:
```
WARNING HTTP error for https://api.morrisons.com/product/v1/items/...: 401 Client Error
```

**Diagnosis**:
Run `python3 utils/test_morrisons_api.py` to identify:
1. Is the API key valid?
2. Is the bearer token being fetched?
3. Which authentication method works?

**Solutions**:
- Update `morrisons_api_key` in config.json
- Verify bearer token Gist URL is correct
- Check if bearer token has expired and needs refresh

### Issue: Bearer Token Fetch Fails

**Symptoms**:
```
ERROR Failed to fetch bearer token from gist https://...: <error>
```

**Diagnosis**:
1. Check network connectivity to GitHub
2. Verify Gist URL is correct and public
3. Confirm Gist contains only the token (no extra whitespace/formatting)

**Solutions**:
- Test Gist URL manually in browser
- Ensure Gist is public (not private)
- Update `morrisons_bearer_token_url` in config.json

### Issue: Stock Enrichment Not Working

**Symptoms**:
- No stock data in INF reports
- No location information displayed

**Diagnosis**:
Run `python3 utils/test_morrisons_config.py` to check:
1. Is `enrich_stock_data` set to `true`?
2. Are API credentials configured?
3. Can bearer token be fetched?

**Solutions**:
- Set `enrich_stock_data: true` in config.json
- Add Morrisons API credentials
- Ensure store numbers are populated in urls.csv

---

## Requirements

Both scripts require:
- `config.json` at project root
- `requests` library installed
- `stock_enrichment` module (for bearer token fetch)

Install dependencies:
```bash
pip install -r requirements.txt
```

---

## Example Workflow

### Setting Up Morrisons API for the First Time

1. **Add credentials to config.json**:
   ```json
   {
     "morrisons_api_key": "YOUR_API_KEY",
     "morrisons_bearer_token_url": "https://gist.githubusercontent.com/.../raw/...",
     "enrich_stock_data": true
   }
   ```

2. **Verify configuration**:
   ```bash
   python3 utils/test_morrisons_config.py
   ```
   Expected: All items should show ✓ SET

3. **Test API connectivity**:
   ```bash
   python3 utils/test_morrisons_api.py
   ```
   Expected: Product API should return 200 SUCCESS

4. **Run scraper**:
   ```bash
   python scraper.py
   ```
   Check logs for stock enrichment messages

### Troubleshooting API Issues

1. **Check current status**:
   ```bash
   python3 utils/test_morrisons_api.py
   ```

2. **Review diagnostic output**:
   - Look for 401/403 errors
   - Check bearer token fetch status
   - Note which endpoints fail

3. **Apply fixes**:
   - Update expired bearer token in Gist
   - Verify API key is current
   - Check GitHub Gist accessibility

4. **Re-test**:
   ```bash
   python3 utils/test_morrisons_api.py
   ```
   Confirm all tests pass

---

## Notes

- These are **diagnostic tools only** - they don't modify any data
- Safe to run multiple times
- Can be run independently of the main scraper
- Output is designed for human readability with color indicators

## Related Documentation

- [MORRISONS_API_FIX.md](../docs/MORRISONS_API_FIX.md) - Detailed fix guide for 401 errors
- [STOCK_ENRICHMENT.md](../STOCK_ENRICHMENT.md) - Stock enrichment feature documentation
- [README.md](../README.md) - Main project documentation
