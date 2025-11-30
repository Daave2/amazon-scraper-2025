# Quick Setup Summary - Interactive Workflow Triggers

## 🎯 What This Does

Adds interactive buttons to your Google Chat reports that let anyone (even without GitHub access) trigger workflows on demand.

**Example:** After a scheduled run completes, the Job Summary card will have buttons like:
- 🔍 Run INF Analysis (Today)
- 📊 Performance Check  
- 📅 Yesterday's INF Report
- 📊 Top 10 INF Items

Click a button → Workflow runs → Results post to Chat!

---

## ⚡ Quick Setup (5 Steps)

### 1. Create GitHub Personal Access Token

1. Go to: https://github.com/settings/tokens?type=beta
2. Click **Generate new token**
3. Settings:
   - **Repository access:** Only select repositories → `Daave2/amazon-scraper`
   - **Permissions → Repository permissions → Actions:** Read and write ✅
4. Click **Generate token**
5. **Copy the token** (you won't see it again!)

### 2. Deploy Apps Script

1. Go to: https://script.google.com
2. Click **New Project**
3. Name it: "Amazon Scraper Triggers"
4. Replace `Code.gs` with the code from: `apps-script/Code.gs`
5. Click **Project Settings** (gear icon) → **Script Properties**
6. Add property:
   - Name: `GH_PAT`
   - Value: (paste your GitHub token from Step 1)
7. Click **Deploy** → **New deployment**
8. Type: **Web app**
9. Settings:
   - Execute as: **Me**
   - Who has access: **Anyone with the link**
10. Click **Deploy**
11. **Copy the web app URL** (looks like `https://script.google.com/macros/s/.../exec`)

### 3. Add Apps Script URL to GitHub Secrets

1. Go to: https://github.com/Daave2/amazon-scraper/settings/secrets/actions
2. Click **New repository secret**
3. Name: `APPS_SCRIPT_WEBHOOK_URL`
4. Value: (paste the URL from Step 2)
5. Click **Add secret**

### 4. Update Workflow to Use Apps Script URL

The workflow needs to add the Apps Script URL to `config.json` during the build step.

In `.github/workflows/run-scraper.yml`, update the Build runtime config step:

```yaml
- name: Build runtime config from Secrets
  env:
    # ... existing env vars ...
    APPS_SCRIPT_WEBHOOK_URL: ${{ secrets.APPS_SCRIPT_WEBHOOK_URL }}
  run: |
    cat > config.json <<JSON
    {
      "debug": false,
      # ... existing config ...
      "apps_script_webhook_url": "${APPS_SCRIPT_WEBHOOK_URL}"
    }
    JSON
```

### 5. Test It!

**Option A: Quick Test (Browser)**
Open this URL in your browser (replace with your actual URL):
`YOUR_APPS_SCRIPT_URL?event_type=run-inf-analysis&date_mode=today`

**Expected:**
- A page saying "✅ Workflow Triggered!"
- A new workflow run starting in GitHub Actions

**Option B: Test Button Click**
1. Run the scraper workflow manually.
2. Wait for the Job Summary card.
3. Click a button.
4. A new browser tab should open, trigger the workflow, and auto-close after 3 seconds.

---

## 🧪 Full Testing

See [TESTING_GUIDE.md](./TESTING_GUIDE.md) for comprehensive testing instructions.

---

## 🔧 Updating the Workflow Config

If you haven't updated the workflow yet, here's the exact change needed:

### File: `.github/workflows/run-scraper.yml`

**Find this section** (around line 128-161):

```yaml
- name: Build runtime config from Secrets
  env:
    FORM_URL:        ${{ secrets.FORM_URL }}
    LOGIN_URL:       ${{ secrets.LOGIN_URL }}
    SECRET_KEY:      ${{ secrets.SECRET_KEY }}
    LOGIN_EMAIL:     ${{ secrets.LOGIN_EMAIL }}
    LOGIN_PASSWORD:  ${{ secrets.LOGIN_PASSWORD }}
    OTP_SECRET_KEY:  ${{ secrets.OTP_SECRET_KEY }}
    CHAT_WEBHOOK_URL: ${{ secrets.CHAT_WEBHOOK_URL }}
    MORRISONS_API_KEY: ${{ secrets.MORRISONS_API_KEY }}
    MORRISONS_BEARER_TOKEN_URL: ${{ secrets.MORRISONS_BEARER_TOKEN_URL }}
```

**Add this line to the `env:` section:**

```yaml
    APPS_SCRIPT_WEBHOOK_URL: ${{ secrets.APPS_SCRIPT_WEBHOOK_URL }}
```

**Then in the JSON generation part, add:**

```json
{
  "debug": false,
  "form_url":        "${FORM_URL}",
  "login_url":       "${LOGIN_URL}",
  "secret_key":      "${SECRET_KEY}",
  "login_email":     "${LOGIN_EMAIL}",
  "login_password":  "${LOGIN_PASSWORD}",
  "otp_secret_key":  "${OTP_SECRET_KEY}",
  "chat_webhook_url": "${CHAT_WEBHOOK_URL}",
  "apps_script_webhook_url": "${APPS_SCRIPT_WEBHOOK_URL}",  # ← ADD THIS LINE
  # ... rest of config ...
}
```

---

## 📋 Checklist

Before going live, make sure:

- [ ] GitHub PAT created with Actions permissions
- [ ] PAT stored in Apps Script Properties as `GH_PAT`
- [ ] Apps Script deployed as web app (Access: "Everyone within Morrisons" or "Anyone")
- [ ] Apps Script URL copied
- [ ] `APPS_SCRIPT_WEBHOOK_URL` secret added to GitHub
- [ ] Workflow updated to use the secret in config.json
- [ ] Tested via browser URL (Option A)
- [ ] Main workflow ran successfully and posted Job Summary card
- [ ] Job Summary card shows Quick Actions section with 4 buttons

---

## 🎉 What's Next

Once setup is complete:

1. **Run a scheduled scrape** (or trigger manually from GitHub)
2. **Check Google Chat** for the Job Summary card
3. **Click a button** to test the interactive triggers
4. **Watch it work!** 🚀

The system is designed to be:
- ✅ **Simple**: Non-GitHub users just click buttons
- ✅ **Secure**: PAT stored safely, not in code
- ✅ **Flexible**: Easy to add more buttons/workflows later
- ✅ **Reliable**: Full error handling and logging

---

## 📚 Additional Resources

- **[apps-script/README.md](./apps-script/README.md)** - Detailed Apps Script setup and maintenance
- **[TESTING_GUIDE.md](./TESTING_GUIDE.md)** - Comprehensive testing procedures
- **[implementation_plan.md](../path/to/implementation_plan.md)** - Full technical design

---

## 🆘 Getting Help

**Problem:** Buttons don't appear in cards
- Check: Is `APPS_SCRIPT_WEBHOOK_URL` in the workflow config?
- Check: Did the workflow run after adding the secret?
- Check: Look at workflow logs for the config.json content

**Problem:** Buttons don't work when clicked
- Check: Does the browser tab open?
- Check: Does the page say "Workflow Triggered"?
- Check: Apps Script execution logs (Apps Script → Executions tab)
- Check: Is PAT still valid?

**Problem:** Workflow doesn't start
- Check: GitHub Actions tab for new runs
- Check: Apps Script logs for errors
- Test: Direct repository_dispatch API call with curl

For detailed troubleshooting, see: [TESTING_GUIDE.md](./TESTING_GUIDE.md#troubleshooting)
