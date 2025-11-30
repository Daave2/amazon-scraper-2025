# Testing Guide - Interactive Workflow Triggers

This guide will walk you through testing the interactive workflow trigger system step by step.

## Prerequisites

Before you begin testing, make sure you have:

- ✅ GitHub Personal Access Token (PAT) with Actions permissions
- ✅ Apps Script deployed as web app
- ✅ Apps Script URL ready (looks like `https://script.google.com/macros/s/.../exec`)
- ✅ PAT stored in Apps Script Properties as `GH_PAT`

## Phase 1: Test repository_dispatch API Directly

This tests that GitHub can receive and process repository_dispatch events.

### Step 1: Set up environment

```bash
# Export your GitHub PAT (don't commit this!)
export GITHUB_PAT="github_pat_YOUR_TOKEN_HERE"

# Test variables
export GITHUB_OWNER="Daave2"
export GITHUB_REPO="amazon-scraper"
```

### Step 2: Test INF Analysis Trigger

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_PAT" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/$GITHUB_OWNER/$GITHUB_REPO/dispatches \
  -d '{
    "event_type": "run-inf-analysis",
    "client_payload": {
      "date_mode": "today",
      "top_n": 5,
      "requested_by": "Test User (curl)",
      "source": "manual-test"
    }
  }'
```

**Expected Result:**
- HTTP 204 No Content (success!)
- Check GitHub Actions → you should see a new workflow run starting
- Event name should show as "repository_dispatch"

### Step 3: Test Full Scrape Trigger

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_PAT" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/$GITHUB_OWNER/$GITHUB_REPO/dispatches \
  -d '{
    "event_type": "run-full-scrape",
    "client_payload": {
      "date_mode": "yesterday",
      "requested_by": "Test User (curl)",
      "source": "manual-test"
    }
  }'
```

**Expected Result:**
- HTTP 204 No Content
- New workflow run in GitHub Actions
- Check the logs - should show "requested by: Test User (curl)"

### Step 4: Test with Custom Date Range

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $GITHUB_PAT" \
  -H "X-GitHub-Api-Version: 2022-11-28"\
  https://api.github.com/repos/$GITHUB_OWNER/$GITHUB_REPO/dispatches \
  -d '{
    "event_type": "run-inf-analysis",
    "client_payload": {
      "date_mode": "custom",
      "custom_start_date": "11/01/2025",
      "custom_end_date": "11/30/2025",
      "top_n": 10,
      "requested_by": "Test User (custom dates)",
      "source": "manual-test"
    }
  }'
```

**Expected Result:**
- Workflow runs with custom date range
- Check logs for date parameters

---

## Phase 2: Test Apps Script Endpoint

This tests that Apps Script can receive requests and trigger GitHub workflows.

### Step 1: Test Apps Script Health Check

Replace `YOUR_DEPLOYMENT_URL` with your actual Apps Script web app URL:

```bash
export APPS_SCRIPT_URL="https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec"

# Test with a simple message
curl -X POST \
  -H "Content-Type: application/json" \
  "$APPS_SCRIPT_URL" \
  -d '{
    "message": {
      "text": "help",
      "sender": {"displayName": "Test User"}
    }
  }'
```

**Expected Result:**
- JSON response with help text
- Should list available commands

### Step 2: Test INF Analysis Trigger via Apps Script

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  "$APPS_SCRIPT_URL" \
  -d '{
    "type": "CARD_CLICKED",
    "action": {
      "actionMethodName": "triggerWorkflow",
      "parameters": [
        {"key": "event_type", "value": "run-inf-analysis"},
        {"key": "date_mode", "value": "today"},
        {"key": "top_n", "value": "5"}
      ]
    },
    "message": {
      "sender": {"displayName": "Test User via Apps Script"}
    },
    "space": {
      "displayName": "Test Space"
    }
  }'
```

**Expected Result:**
- JSON response with success card
- Should say "Workflow Triggered"
- Check GitHub Actions - new workflow run should start
- Check Apps Script logs (Executions tab) - should show successful trigger

### Step 3: Test Text Command

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  "$APPS_SCRIPT_URL" \
  -d '{
    "message": {
      "text": "run inf",
      "sender": {"displayName": "Test User"}
    }
  }'
```

**Expected Result:**
- JSON response with success card
- GitHub workflow starts

### Step 4: Test Error Handling

Test with missing PAT (temporarily remove it from Script Properties):

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  "$APPS_SCRIPT_URL" \
  -d '{
    "type": "CARD_CLICKED",
    "action": {
      "parameters": [
        {"key": "event_type", "value": "run-inf-analysis"}
      ]
    },
    "message": {
      "sender": {"displayName": "Test User"}
    }
  }'
```

**Expected Result:**
- Error response mentioning PAT not configured
- No workflow starts

---

## Phase 3: Test End-to-End Flow

This is the full user experience test.

### Prerequisites

- Apps Script URL added as secret `APPS_SCRIPT_WEBHOOK_URL` in GitHub
- Workflow updated to include this in `config.json`
- Main scraper workflow has run at least once to test cards

### Test Steps

1. **Trigger Main Scraper:**
   - Go to GitHub Actions
   - Run workflow manually (or wait for scheduled run)
   - Wait for completion

2. **Check Google Chat:**
   - Look for the Job Summary card
   - Should have "⚡ Quick Actions" section
   - Should see 4 buttons:
     - 🔍 Run INF Analysis (Today)
     - 📊 Performance Check
     - 📅 Yesterday's INF Report
     - 📊 Top 10 INF Items

3. **Click "Run INF Analysis (Today)":**
   - Card should update with "Workflow Triggered" message
   - Or get inline response
   - Check GitHub Actions - new "Manual INF Analysis" workflow should start
   - Wait for completion
   - INF report should post to Google Chat

4. **Click "Yesterday's INF Report":**
   - Same as above, but with yesterday's data
   - Check workflow logs - should show `date_mode: yesterday`

5. **Click "Top 10 INF Items":**
   - Should trigger INF analysis with `top_n: 10`
   - Report should show 10 items per store instead of 5

---

## Troubleshooting

### GitHub API returns 404

**Problem:** `curl` returns 404 Not Found

**Solutions:**
- Check `GITHUB_OWNER` and `GITHUB_REPO` are correct
- Verify your PAT has access to the repository
- Make sure the repository is not private (or PAT has private repo access)

### GitHub API returns 401/403

**Problem:** Unauthorized or Forbidden

**Solutions:**
- Verify your PAT is valid and not expired
- Check PAT permissions include "Actions: Read and write"
- Try generating a new PAT

### Workflow doesn't start

**Problem:** API returns 204 but no workflow appears

**Solutions:**
- Check the workflow YAML has `repository_dispatch` trigger
- Verify the `event_type` matches exactly (case-sensitive!)
- Look at workflow file for the specific `types:` list
- Check GitHub Actions tab → some workflows might be filtered

### Apps Script returns error

**Problem:** Apps Script execution fails

**Solutions:**
1. **Check Execution Logs:**
   - Apps Script editor → Executions (clock icon)
   - Click on the failed execution
   - Read the error message

2. **Common Issues:**
   - "GH_PAT not configured" → Add PAT to Script Properties
   - "Unauthorized" → PAT is invalid or wrong permissions
   - "404" → Check GITHUB_OWNER/GITHUB_REPO in Code.gs
   - Timeout → GitHub API might be slow, increase timeout in Apps Script

### Buttons don't appear in Google Chat

**Problem:** Job Summary card has no Quick Actions section

**Solutions:**
- Check `APPS_SCRIPT_WEBHOOK_URL` secret exists in GitHub
- Verify workflow build step includes `apps_script_webhook_url` in config.json
- Check workflow logs - config.json should show the URL
- Try manually adding the URL to a test config.json and run locally

### Buttons appear but don't work

**Problem:** Click button, nothing happens

**Solutions:**
1. **Check Google Chat App Configuration:**
   - Make sure the Chat app is configured with the Apps Script URL
   - Verify "Interactive features" are enabled

2. **Test Apps Script directly:**
   - Use curl (Phase 2 tests above)
   - Check if Apps Script can receive the request

3. **Check Card Format:**
   - Button `onClick.action` must have `function` and `parameters`
   - Parameters must use `key` and `value` format

### Workflow runs but uses wrong parameters

**Problem:** Workflow starts but doesn't use client_payload values

**Solutions:**
- Check workflow YAML - make sure it reads from `github.event.client_payload`
- Look for this pattern: `${{ github.event.client_payload.date_mode }}`
- Check workflow logs - should show "Repository dispatch trigger"
- Verify client_payload is logged

---

## Verification Checklist

Use this checklist to verify everything is working:

- [ ] Can trigger workflow via curl (repository_dispatch API)
- [ ] Apps Script responds to curl test
- [ ] Apps Script can trigger GitHub workflow
- [ ] GitHub workflow receives client_payload correctly
- [ ] Job Summary card shows Quick Actions buttons
- [ ] Clicking buttons triggers Apps Script
- [ ] Apps Script logs show successful triggers
- [ ] GitHub workflows start from button clicks
- [ ] Workflow logs show correct parameters (date_mode, top_n, etc.)
- [ ] INF reports complete and post to Chat
- [ ] All 4 buttons work independently

---

## Example Success Flow

Here's what a complete successful test looks like:

1. **Trigger:** User clicks "Run INF Analysis (Today)" in Google Chat
2. **Google Chat → Apps Script:** Button sends action to Apps Script URL
3. **Apps Script Logs:** Shows request received, triggering GitHub workflow
4. **Apps Script → GitHub:** Calls repository_dispatch API
5. **GitHub API:** Returns 204 No Content
6. **Apps Script → Google Chat:** Posts "Workflow Triggered" card
7. **GitHub Actions:** "Manual INF Analysis" workflow starts
8. **Workflow logs:** Shows "requested_by: [User Name]", "date_mode: today"
9. **Workflow completes:** INF scraper runs successfully
10. **Google Chat:** INF report appears with top 5 items per store

**Total time:** ~2-5 minutes depending on workflow complexity

---

## Next Steps After Testing

Once all tests pass:

1. **Remove test payloads** - Don't leave test workflow runs cluttering GitHub Actions
2. **Document for team** - Share the button usage with non-GitHub users
3. **Monitor usage** - Check Apps Script executions regularly
4. **Rotate PAT** - Consider PAT expiration policy
5. **Add more buttons** - Consider adding buttons to other cards (Performance Highlights, INF Reports)

---

## Advanced: Debugging Live Requests

To debug issues with live button clicks in Google Chat:

1. **Enable verbose logging in Apps Script:**
   ```javascript
   Logger.log('Full request body: ' + JSON.stringify(body));
   Logger.log('Action parameters: ' + JSON.stringify(body.action?.parameters));
   ```

2. **Check Google Chat app logs:**
   - Google Cloud Console → your project → Logs Explorer
   - Filter by your Chat app

3. **Monitor GitHub Actions in real-time:**
   - GitHub Actions tab → enable auto-refresh
   - Watch for new workflow runs as you click buttons

4. **Use Apps Script's test function:**
   ```javascript
   function testTrigger() {
     const result = triggerGitHubWorkflow('run-inf-analysis', {
       date_mode: 'today',
       requested_by: 'Manual Test',
       source: 'apps-script-test'
     });
     Logger.log('Result: ' + JSON.stringify(result));
   }
   ```
   Run this from Apps Script editor (Run button) to test without Google Chat.
