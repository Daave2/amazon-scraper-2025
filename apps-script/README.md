# Apps Script Setup Guide

This directory contains the Google Apps Script code that bridges Google Chat and GitHub Actions.

## 📋 Prerequisites

1. **GitHub Personal Access Token (PAT)**
   - Go to GitHub Settings → Developer settings → Personal access tokens → Fine-grained tokens
   - Click "Generate new token"
   - Repository access: Select "Only select repositories" → Choose `Daave2/amazon-scraper`
   - Permissions → Repository permissions → Actions: **Read and write**
   - Generate token and copy it (you won't see it again!)

2. **Google Apps Script Project**
   - Go to [script.google.com](https://script.google.com)
   - Create a new project
   - Name it something like "Amazon Scraper Triggers"

## 🚀 Deployment Steps

### Step 1: Create the Apps Script

1. Open your new Apps Script project
2. Replace the default `Code.gs` content with the code from `Code.gs` in this directory
3. Review the configuration at the top:
   ```javascript
   const GITHUB_OWNER = 'Daave2';
   const GITHUB_REPO = 'amazon-scraper';
   ```

### Step 2: Store GitHub PAT Securely

1. In Apps Script editor, click on Project Settings (gear icon)
2. Scroll to "Script Properties"
3. Click "Add script property"
4. Name: `GH_PAT`
5. Value: Paste your GitHub Personal Access Token
6. Click "Save script property"

### Step 3: Deploy as Web App

1. Click "Deploy" → "New deployment"
2. Click gear icon → Select "Web app"
3. Configuration:
   - **Description:** "GitHub Workflow Trigger v1"
   - **Execute as:** Me (your email)
   - **Who has access:** Anyone with the link
4. Click "Deploy"
5. **Copy the deployment URL** - you'll need this for the config!
   - It looks like: `https://script.google.com/macros/s/DEPLOYMENT_ID/exec`

### Step 4: Test the Deployment

You can test the endpoint with curl:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  "YOUR_DEPLOYMENT_URL_HERE" \
  -d '{
    "type": "CARD_CLICKED",
    "action": {
      "actionMethodName": "triggerWorkflow",
      "parameters": [
        {"key": "event_type", "value": "run-inf-analysis"},
        {"key": "date_mode", "value": "today"}
      ]
    },
    "message": {
      "sender": {"displayName": "Test User"}
    }
  }'
```

Expected response:
```json
{
  "cardsV2": [{
    "card": {
      "header": {
        "title": "✅ Workflow Triggered"
      }
    }
  }]
}
```

### Step 5: Update GitHub Secrets

Add the Apps Script webhook URL to your GitHub secrets:

1. Go to GitHub repository → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `APPS_SCRIPT_WEBHOOK_URL`
4. Value: Your deployment URL from Step 3
5. Click "Add secret"

The workflow will use this to add buttons to the cards.

## 🔧 Maintenance

### Viewing Logs

1. In Apps Script editor, click "Executions" (clock icon on left sidebar)
2. Click on any execution to see detailed logs
3. Use `Logger.log()` statements in the code for debugging

### Updating the Code

1. Make changes in the Apps Script editor
2. Save (Ctrl+S or Cmd+S)
3. Create a new deployment OR update the existing one:
   - Click "Deploy" → "Manage deployments"
   - Click edit icon (pencil) on your web app deployment
   - Update version: "New version"
   - Click "Deploy"

### Revoking/Rotating PAT

If your PAT is compromised:
1. Revoke the old token in GitHub
2. Generate a new one
3. Update the `GH_PAT` script property in Apps Script
4. No need to redeploy!

## 📝 Supported Commands

### Card Button Parameters

Apps Script responds to these `event_type` values:
- `run-inf-analysis` - Triggers INF analysis workflow
- `run-performance-check` - Triggers performance analysis
- `run-full-scrape` - Triggers full scraper run

### Additional Parameters

- `date_mode`: today, yesterday, last_7_days, last_30_days, week_to_date
- `top_n`: 5, 10, 25 (for INF analysis only)

### Text Commands (Optional)

You can also type these in Google Chat:
- "run inf" - Runs INF analysis for today
- "run performance" - Runs performance check
- "run full scrape" - Runs full scraper
- "help" - Shows available commands

## ⏱️ Cooldown Protection

- Button clicks and text commands are rate-limited to **one trigger every 30 minutes per workflow**.
- When a request is blocked, the response explains who triggered the last run and how long to wait.
- This prevents duplicate executions from double-clicks or multiple users trying to start the same job simultaneously.

## 👮 User Whitelist (Configurable)

The whitelist controls who can trigger workflows via the Quick Actions buttons.

### Option 1: Configure via Script Properties (Recommended)

You can enable/disable and manage the whitelist without redeploying:

1. In Apps Script editor, go to **Project Settings** (gear icon)
2. Scroll to **Script Properties**
3. Add these properties:

| Property Name | Value | Description |
|--------------|-------|-------------|
| `WHITELIST_ENABLED` | `true` or `false` | Set to `false` to disable whitelist (allow everyone) |
| `WHITELIST_EMAILS` | `["email1@example.com", "email2@example.com"]` | JSON array of authorized emails |

**Example:**
- **WHITELIST_ENABLED**: `true`
- **WHITELIST_EMAILS**: `["niki.cooke@morrisonsplc.co.uk", "another.user@morrisonsplc.co.uk"]`

### Option 2: Use Default Hardcoded Whitelist

If you don't set the Script Properties, the system will use the default:
- Whitelist is **enabled** by default
- Only `niki.cooke@morrisonsplc.co.uk` is authorized

### How It Works

- **Whitelist Enabled** (default): Only users in the whitelist can trigger workflows
- **Whitelist Disabled**: Anyone with the link can trigger workflows (use with caution!)
- Unauthorized users see an "Access Denied" message
- All trigger attempts are logged with user attribution

### To Disable the Whitelist

Set `WHITELIST_ENABLED` to `false` in Script Properties. This allows anyone with the deployment URL to trigger workflows.

### To Add/Remove Users

Update the `WHITELIST_EMAILS` property with a JSON array of authorized emails. Changes take effect immediately - no redeployment needed!

## 🔒 Security Notes

- ✅ PAT is stored in Script Properties (not in code)
- ✅ Deploy as "Execute as: Me" so it runs with your credentials
- ✅ "Anyone with the link" is safe - the URL is secret and not guessable
- ✅ All triggers are logged with user attribution
- ⚠️ Never commit the PAT to Git
- ⚠️ Never share the deployment URL publicly

## ❓ Troubleshooting

### "GitHub PAT not configured"
- Check that you added `GH_PAT` to Script Properties
- Make sure there are no extra spaces in the PAT value

### "API returned 404"
- Verify `GITHUB_OWNER` and `GITHUB_REPO` are correct in `Code.gs`
- Check that your PAT has access to the repository

### "API returned 401" or "403"
- Your PAT may be invalid or expired
- Verify the PAT has "Actions: Read and write" permission
- Try generating a new PAT

### Workflow doesn't start
- Check GitHub Actions → verify the workflow file has `repository_dispatch` trigger
- Check the execution logs in Apps Script for errors
- Verify the `event_type` matches what's in the workflow YAML

### No response in Google Chat
- Check Apps Script execution logs for errors
- Make sure the deployment URL is correct
- Try testing with curl first

## 🔗 Useful Links

- [Apps Script Documentation](https://developers.google.com/apps-script)
- [GitHub repository_dispatch API](https://docs.github.com/en/rest/repos/repos#create-a-repository-dispatch-event)
- [Google Chat Card Format](https://developers.google.com/chat/api/guides/message-formats/cards)
