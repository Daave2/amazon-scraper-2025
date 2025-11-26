# GitHub Actions Workflows - Date Range Options

This directory contains multiple workflow files for different data collection schedules and date ranges.

## Available Workflows

### 1. `run-scraper.yml` - Main Scraper (Today's Data)

**Purpose**: Primary workflow for real-time data collection throughout the day.

**Schedule**: Every hour, runs only at specified UK times (09:00, 12:00, 14:00, 17:00, 20:00)

**Date Range**: Today's data (default)

**Manual Trigger Options**:
When manually triggering this workflow, you can select:
- **Date Mode**:
  - `today` - Current day's data (default)
  - `yesterday` - Previous day's data
  - `week_to_date` - Monday through today
  - `relative` - Custom offset from today
  - `custom` - Specific date range
- **Relative Days**: For relative mode (e.g., `-1` for yesterday, `-7` for last week)
- **Custom Dates**: Start and end dates for custom mode (MM/DD/YYYY format)

**Example Manual Triggers**:
```
Date Mode: yesterday          → Gets yesterday's data
Date Mode: week_to_date       → Gets Monday through today
Date Mode: relative           → Gets data for specified offset
Relative Days: -7             → 7 days ago
Date Mode: custom             → Gets specific date range
Custom Start: 01/15/2025
Custom End: 01/20/2025
```

---

### 2. `yesterday-report.yml` - Yesterday's Data

**Purpose**: Automated daily report of previous day's complete data.

**Schedule**: Every day at 8:00 AM UK time

**Date Range**: Yesterday (relative -1 day)

**Use Cases**:
- Morning performance reviews
- Historical data collection
- Comparing today vs. yesterday
- Daily trending analysis

**Why 8 AM?**
- Ensures previous day's data is complete
- Provides morning team with yesterday's summary
- Available for morning standup meetings

---

### 3. `week-to-date-report.yml` - Weekly Summary

**Purpose**: Aggregated data from Monday through current day.

**Schedule**: Monday-Friday at 9:00 PM UK time

**Date Range**: Monday of current week through today

**Use Cases**:
- Weekly performance tracking
- Week-over-week comparisons
- End-of-day weekly summaries
- Identifying weekly trends

**Why Monday-Friday at 9 PM?**
- Captures full day's data
- End-of-day summary for management
- Excludes weekends (typically lower volume)
- Available for next-morning review

**Week Calculation**:
- Week starts on Monday (ISO standard)
- Includes all days from Monday through today
- Automatically calculates date range

---

## Workflow Comparison

| Workflow | Schedule | Date Range | Primary Use |
|----------|----------|------------|-------------|
| **Main Scraper** | Hourly (specific times) | Today | Real-time tracking |
| **Yesterday Report** | Daily 8 AM | Yesterday | Historical review |
| **Week-to-Date** | Weeknights 9 PM | Monday-Today | Weekly trending |

---

## Manual Triggering

All workflows can be triggered manually via the GitHub Actions tab:

1. Go to **Actions** tab in GitHub
2. Select desired workflow from left sidebar
3. Click **Run workflow** button
4. (Main workflow only) Select date mode and parameters
5. Click **Run workflow**

---

## Configuration

All workflows use the same GitHub Secrets:

### Required Secrets

- `FORM_URL` - Google Form submission endpoint
- `LOGIN_URL` - Amazon Seller Central login URL
- `SECRET_KEY` - Encryption key
- `LOGIN_EMAIL` - Amazon account email
- `LOGIN_PASSWORD` - Amazon account password
- `OTP_SECRET_KEY` - TOTP secret for 2FA
- `CHAT_WEBHOOK_URL` - Google Chat webhook URL
- `MORRISONS_API_KEY` - Morrisons API key
- `MORRISONS_BEARER_TOKEN_URL` - Bearer token fetch URL

Configure these in: **Settings → Secrets and variables → Actions**

---

## Artifact Outputs

Each workflow uploads:
- **Logs**: `app.log` with detailed execution logs
- **Output**: CSV files and screenshots in `output/` directory
- **Auth State**: `state.json` for session persistence

**Retention**: 7 days

**Naming**:
- Main: `scraper-output-{run_id}`
- Yesterday: `scraper-output-yesterday-{run_id}`
- Week-to-Date: `scraper-output-week-to-date-{run_id}`

---

## Recommended Schedule

For comprehensive coverage, use all three workflows:

**Daily**:
- 08:00 AM - Yesterday Report (previous day complete data)
- 09:00 AM - Main scraper (today's data)
- 12:00 PM - Main scraper (midday update)
- 02:00 PM - Main scraper (afternoon check)
- 05:00 PM - Main scraper (end-of-business)
- 08:00 PM - Main scraper (final update)
- 09:00 PM - Week-to-Date (Mon-Fri weekly summary)

This provides:
- ✅ Complete historical data (yesterday)
- ✅ Real-time tracking throughout day (today)
- ✅ Weekly trending (week-to-date)

---

## Advanced Usage

### Custom One-Off Reports

Use the main workflow's manual trigger for ad-hoc reports:

**Last Week's Data**:
```
Date Mode: custom
Start Date: [Monday of last week]
End Date: [Sunday of last week]
```

**Month-to-Date**:
```
Date Mode: custom
Start Date: [First day of month]
End Date: [Today]
```

**Specific Week**:
```
Date Mode: custom
Start Date: 01/20/2025
End Date: 01/26/2025
```

### Modifying Schedules

Edit the `schedule` section in each YAML file:

```yaml
schedule:
  - cron: '0 8 * * *'  # Minute Hour Day Month DayOfWeek
```

**Cron Format**:
- `0 8 * * *` = 8:00 AM UTC every day
- `0 20 * * 1-5` = 8:00 PM UTC Monday-Friday
- `0 */2 * * *` = Every 2 hours

**Timezone Note**: GitHub Actions uses UTC. Adjust for UK time:
- Winter (GMT): UTC = UK time
- Summer (BST): UTC + 1 = UK time

---

## Troubleshooting

### Workflow Not Running

1. **Check Schedule**: Verify cron expression is correct
2. **Check Branch**: Workflows only run on default branch
3. **Check Secrets**: Ensure all required secrets are configured
4. **Check Logs**: Review workflow run logs in Actions tab

### Date Range Issues

1. **Verify Format**: Dates must be MM/DD/YYYY
2. **Check Timezone**: All dates use Europe/London timezone
3. **Test Manually**: Use manual trigger to test date logic
4. **Review Logs**: Check scraper logs for date parsing errors

### Authentication Failures

1. **Check Secrets**: Verify credentials are current
2. **Check Auth State**: Previous run may have cached invalid state
3. **Manual Run**: Trigger manually to force re-auth
4. **Clear Cache**: Delete `auth-state` artifact and re-run

---

## Related Documentation

- [README.md](../README.md) - Main project documentation
- [DATE_RANGE_FEATURE.md](../DATE_RANGE_FEATURE.md) - Date range implementation details
- [docs/](../docs/) - Additional documentation

---

**Last Updated**: 2025-11-26
