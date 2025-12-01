# On-Demand Workflow Triggers via Google Chat

## Overview

The scraper now supports on-demand report triggering via Google Chat buttons, powered by Google Apps Script. This allows you to run any report immediately without waiting for scheduled runs.

## Architecture

```
Google Chat Button → Apps Script → GitHub API → Repository Dispatch → Workflow Runs
```

1. User clicks button in Google Chat
2. Apps Script receives request and validates it
3. Apps Script triggers GitHub workflow via `repository_dispatch` API
4. Workflow runs with specified parameters
5. Report posts back to Google Chat when complete

## Quick Actions Card

After each automated run, a "Quick Actions" card appears with 4 buttons:

### 1. 🔍 INF Analysis (Today)
- **What it does**: Top 10 INF items per store for today's data
- **Event**: `run-inf-analysis` with `date_mode=today` and `top_n=10`
- **Workflow**: `manual-inf-analysis.yml`

### 2. 📊 Performance Check
- **What it does**: Lates, UPH, and key performance metrics for today
- **Event**: `run-performance-check` with `date_mode=today`
- **Workflow**: `manual-performance-analysis.yml`

### 3. 📅 Yesterday's INF Report
- **What it does**: Top 10 INF items per store from yesterday
- **Event**: `run-inf-analysis` with `date_mode=yesterday` and `top_n=10`
- **Workflow**: `manual-inf-analysis.yml`

### 4. 📊 Week-to-Date INF
- **What it does**: Summary of INF from Monday through today
- **Event**: `run-inf-analysis` with `date_mode=week_to_date` and `top_n=10`
- **Workflow**: `manual-inf-analysis.yml`

## Cooldown Protection

To prevent accidental duplicate runs, each workflow has a 30-minute cooldown period:

- If you trigger a workflow, subsequent requests for the same workflow type are blocked for 30 minutes
- The chat message will show who triggered it and how long until it can be triggered again
- Example: `Workflow recently triggered by user@example.com. Wait 25 minute(s).`

## Automated Schedule

The system now has a simplified, dedicated schedule:

### Daily Automated Reports

| Time | Report | Workflow |
|------|--------|----------|
| **8 AM** | Yesterday's Full INF | `yesterday-report.yml` |
| **12 PM** | Performance Check (Today) | `midday-performance.yml` |
| **2 PM** | Today's INF (So Far) | `afternoon-inf.yml` |

All other reports are available on-demand via Quick Actions buttons.

## Setup

### 1. Deploy Apps Script

1. Copy code from `apps-script/Code.gs`
2. Deploy as Web App
3. Set permissions to "Anyone" (or "Anyone in organization")
4. Copy the deployment URL

### 2. Configure Script Properties

In Apps Script → Project Settings → Script Properties, add:

| Property | Value | Description |
|----------|-------|-------------|
| `GH_PAT` | Your GitHub Personal Access Token | Needs `repo` scope |
| `CHAT_WEBHOOK_URL` | Your Google Chat Webhook URL | For acknowledgement messages |

### 3. Add to GitHub Secrets

In GitHub → Settings → Secrets → Actions, ensure you have:

- `APPS_SCRIPT_WEBHOOK_URL` - The Apps Script deployment URL

### 4. Push Code

Ensure these workflows exist in `.github/workflows/`:
- `manual-inf-analysis.yml`
- `manual-performance-analysis.yml`
- `yesterday-report.yml`
- `midday-performance.yml`
- `afternoon-inf.yml`

## Enhanced INF Reports

### Network-Wide Card

The network-wide INF report now shows:

```
22 - VOSS Still Water, Pack of 12 x 500ml PET Bottles
(5 stores: Cleveleys 10, Woking 4, Halifax 3)
£1.25 | SKU: 7012345
```

**Features:**
- **Total INF count** across all stores
- **Top 3 contributing stores** with individual counts
- **Total store count** affected
- **Price** from Morrisons API (if available)
- **SKU** for easy identification

### Acknowledgement Cards

When you trigger a workflow, an immediate acknowledgement is posted:

```
⏳ Workflow Started
INF Analysis (Today)

user@example.com has requested a report.
Running now, please wait...
```

This provides visibility to everyone in the chat that a report is running.

##

 How It Works

1. **User clicks button** in Google Chat
2. **Apps Script checks cooldown** - blocks if triggered recently
3. **Acknowledgement posted** to chat
4. **GitHub workflow triggered** via API
5. **Workflow runs** and generates report
6. **Report posted** to chat
7. **Quick Actions card** posted again for next request

## Troubleshooting

### Button doesn't work
- Verify Apps Script deployment URL is correct in GitHub Secrets
- Check Apps Script execution logs for errors
- Ensure GitHub PAT has `repo` scope

### Workflow doesn't start
- Check GitHub Actions tab for dispatch events
- Verify repository name matches in Apps Script (`GITHUB_REPO`)
- Check GitHub PAT expiration

### Cooldown not resetting
- Cooldown data stored in Script Properties with key `LAST_TRIGGER_{event_type}`
- Can manually delete via Script Properties if needed

See `apps-script/README.md` for detailed Apps Script documentation.
