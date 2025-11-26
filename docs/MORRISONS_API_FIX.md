# Morrisons API 401 Error - Root Cause and Fix

## Issue Summary
The scraper was getting **401 Unauthorized** errors when calling the Morrisons API in GitHub Actions, despite having the API key configured.

## Root Cause Analysis

### 1. Authentication Requirements
Through diagnostic testing, we discovered the Morrisons API requires **BOTH**:
- ✅ **API Key** (as query parameter: `?apikey=xxx`)
- ✅ **Bearer Token** (as HTTP header: `Authorization: Bearer xxx`)

Testing showed:
- API key alone → 401 Unauthorized
- Bearer token alone → 401 Unauthorized  
- **Both together → 200 Success** ✅

### 2. The Bug: Missing Bearer Token in Production
The GitHub Actions workflow was configured to pass `MORRISONS_BEARER_TOKEN_URL` to the runtime config, but there were two issues:

**Issue #1: Counterproductive Retry Logic**
The `stock_enrichment.py` code had logic to retry API calls WITHOUT the bearer token if a 401 was received:
```python
if r.status_code in (401, 403) and bearer:
    app_logger.debug(f"Bearer token failed for {url}; retrying without it.")
    r = _http_get(url, None)  # ❌ This makes it worse!
```

This retry logic was **counterproductive** because:
- Morrisons API **requires** the bearer token
- Retrying without it would always fail with 401
- It masked the real issue (bearer token not being fetched)

**Issue #2: Insufficient Logging**
When 401 errors occurred, the logs didn't indicate whether the bearer token was present or not, making it hard to diagnose the root cause.

## Fixes Applied

### 1. Enhanced Bearer Token Fetch Logging
**File**: `stock_enrichment.py`
```python
def fetch_bearer_token_from_gist(gist_url: str) -> str | None:
    """Fetch the bearer token from a GitHub gist URL."""
    try:
        app_logger.info(f"Fetching bearer token from: {gist_url}")  # ✅ Show URL
        response = requests.get(gist_url, timeout=10)
        response.raise_for_status()
        token = response.text.strip()
        app_logger.info(f"Successfully fetched bearer token from gist (length: {len(token)})")  # ✅ Show success
        return token
    except Exception as e:
        app_logger.error(f"Failed to fetch bearer token from gist {gist_url}: {e}")  # ✅ Show error
        return None
```

**Benefits:**
- Shows the exact URL being accessed
- Confirms token was fetched and its length
- Errors are logged at ERROR level for visibility

### 2. Removed Counterproductive Retry Logic
**File**: `stock_enrichment.py`
```python
def _fetch_json(url: str, bearer: str | None) -> Dict[str, Any] | None:
    """Fetches and parses JSON from a URL."""
    try:
        r = _http_get(url, bearer)
        # ❌ REMOVED: Retry without bearer token logic
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        if e.response and e.response.status_code == 404:
            return None
        # ✅ NEW: Show auth status in error logs
        auth_status = "WITH bearer token" if bearer else "WITHOUT bearer token"
        app_logger.warning(f"HTTP error for {url} ({auth_status}): {e}")
        return None
```

**Benefits:**
- Doesn't mask auth failures by retrying without required credentials
- Clearly indicates in logs whether bearer token was present
- Makes debugging auth issues much easier

### 3. Updated Local Config
**File**: `config.json`
```json
{
  "morrisons_bearer_token_url": "https://gist.githubusercontent.com/Daave2/b62faeed0dd435100773d4de775ff52d/raw/gistfile1.txt"
}
```

## Verification

### Local Testing
```bash
$ python3 test_morrisons_api.py

============================================================
MORRISONS API DIAGNOSTIC TEST
============================================================

1. Configuration Check:
   API Key: ✓ SET
   Bearer Token URL: ✓ SET

2. Fetching Bearer Token:
   ✓ Token fetched successfully
   Token preview: pBkGWHfOaC52zNC...

3. Testing API Endpoints:
   a) Product API (with bearer token):
      Status: 200
      ✓ SUCCESS - Product data retrieved
```

### Expected Production Logs (GitHub Actions)
With these fixes, the logs should now show:
```
INFO Fetching bearer token from: https://gist.githubusercontent.com/...
INFO Successfully fetched bearer token from gist (length: 27)
INFO Fetching stock & location data for 10 items...
INFO Finished enriching items with Morrisons data.
```

If bearer token fetch fails, you'll see:
```
ERROR Failed to fetch bearer token from gist https://...: <error details>
```

If API calls fail, you'll see:
```
WARNING HTTP error for https://api.morrisons.com/... (WITH bearer token): 401...
```
or
```
WARNING HTTP error for https://api.morrisons.com/... (WITHOUT bearer token): 401...
```

## Important Notes

### Bearer Token Expiration
Morrisons bearer tokens may have a limited lifespan. If you start seeing 401 errors again in the future:
1. Check if the token in your gist has expired
2. Update the gist with a fresh token
3. The scraper will automatically fetch the new token on next run

### GitHub Actions Configuration
The workflow already has the correct configuration:
```yaml
env:
  MORRISONS_BEARER_TOKEN_URL: ${{ secrets.MORRISONS_BEARER_TOKEN_URL }}
```

Make sure the corresponding GitHub secret is set to:
```
https://gist.githubusercontent.com/Daave2/b62faeed0dd435100773d4de775ff52d/raw/gistfile1.txt
```

## Next Steps

1. ✅ **Commit and push** these changes
2. ✅ **Monitor the next GitHub Actions run** to verify bearer token is fetched successfully  
3. ✅ **Check for 200 responses** from Morrisons API instead of 401s
4. 🔄 **Set up token refresh** if bearer tokens expire frequently (future enhancement)
