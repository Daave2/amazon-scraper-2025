# Workflow Date Range Implementation - Summary

## What Was Added

### ✅ Enhanced Main Workflow (`run-scraper.yml`)

**New Manual Trigger Options**:
Added dropdown menu and input fields when manually triggering:

```yaml
workflow_dispatch:
  inputs:
    date_mode:          # Dropdown with 5 options
    relative_days:      # Input for relative offset
    custom_start_date:  # Input for custom start (MM/DD/YYYY)
    custom_end_date:    # Input for custom end (MM/DD/YYYY)
```

**Available Date Modes**:
1. ✅ **today** - Current day (default for scheduled runs)
2. ✅ **yesterday** - Previous day
3. ✅ **week_to_date** - Monday through today
4. ✅ **relative** - Custom day offset (e.g., -7 for last week)
5. ✅ **custom** - Specific date range

**Smart Date Calculation**:
- Automatically calculates Monday of current week for week_to_date
- Uses Europe/London timezone
- Handles both GNU date (Linux) and BSD date (macOS)

---

### ✅ New Workflow: Yesterday's Data (`yesterday-report.yml`)

**Purpose**: Automated daily historical report

**Schedule**:
```yaml
cron: '0 7 * * *'  # 8 AM UK time every day
```

**What it does**:
- Collects complete data from previous day
- Runs every morning at 8 AM UK time
- Perfect for daily review meetings
- Ensures complete 24-hour data capture

**Command**: `python scraper.py --date-mode relative --relative-days -1`

---

### ✅ New Workflow: Week-to-Date (`week-to-date-report.yml`)

**Purpose**: Weekly performance tracking

**Schedule**:
```yaml
cron: '0 20 * * 1-5'  # 9 PM UK time Monday-Friday
```

**What it does**:
- Aggregates data from Monday through current day
- Runs weeknights at 9 PM UK time
- Automatically calculates week start (Monday)
- Skips weekends (Saturday/Sunday)

**Command**: 
```bash
python scraper.py --date-mode custom \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --start-time "12:00 AM" \
  --end-time "11:59 PM"
```

---

## Complete Schedule

### Typical Week

**Monday-Friday**:
```
08:00 AM - Yesterday Report    (previous day complete)
09:00 AM - Main Scraper        (today's data)
12:00 PM - Main Scraper        (midday update)
02:00 PM - Main Scraper        (afternoon)
05:00 PM - Main Scraper        (end of business)
08:00 PM - Main Scraper        (final update)
09:00 PM - Week-to-Date        (weekly summary)
```

**Saturday-Sunday**:
```
08:00 AM - Yesterday Report    (previous day)
(Main scraper can be manually triggered if needed)
```

---

## Usage Examples

### Manual Triggers

#### 1. Get Yesterday's Data
**Via Main Workflow**:
```
Workflow: Run Playwright Scraper
Date Mode: yesterday
→ Click "Run workflow"
```

**Via Dedicated Workflow**:
```
Workflow: Yesterday's Data Report
→ Click "Run workflow"
```

#### 2. Get Week-to-Date
**Via Main Workflow**:
```
Workflow: Run Playwright Scraper
Date Mode: week_to_date
→ Click "Run workflow"
```

**Via Dedicated Workflow**:
```
Workflow: Week-to-Date Report
→ Click "Run workflow"
```

#### 3. Get Last Week's Data
**Via Main Workflow**:
```
Workflow: Run Playwright Scraper
Date Mode: custom
Custom Start Date: 01/20/2025  (last Monday)
Custom End Date: 01/26/2025    (last Sunday)
→ Click "Run workflow"
```

#### 4. Get Specific Day
**Via Main Workflow**:
```
Workflow: Run Playwright Scraper
Date Mode: relative
Relative Days: -3  (3 days ago)
→ Click "Run workflow"
```

---

## Workflow Selection Guide

| Need | Use Workflow | Date Mode |
|------|-------------|-----------|
| **Real-time today** | Main Scraper (scheduled) | today |
| **Yesterday's complete data** | Yesterday Report | relative -1 |
| **This week so far** | Week-to-Date Report | Monday-today |
| **Last week** | Main (manual) | custom |
| **Specific date** | Main (manual) | custom or relative |
| **Month-to-date** | Main (manual) | custom |

---

## Benefits

### 1. **Flexibility**
- ✅ Multiple date modes for different needs
- ✅ Manual override for ad-hoc reports
- ✅ Automated scheduling for routine reports

### 2. **Comprehensive Coverage**
- ✅ Real-time data (main scraper)
- ✅ Historical data (yesterday report)
- ✅ Trending data (week-to-date)

### 3. **User-Friendly**
- ✅ Dropdown menus (no need to remember syntax)
- ✅ Clear descriptions for each option
- ✅ Default values for quick selection

### 4. **Automatic Calculations**
- ✅ Week start automatically calculated
- ✅ Timezone handling (Europe/London)
- ✅ Cross-platform date commands

---

## Technical Details

### Date Command Compatibility

**Linux (GitHub Actions)**:
```bash
START_DATE=$(TZ="Europe/London" date -d "monday" +"%m/%d/%Y")
```

**macOS (local development)**:
```bash
START_DATE=$(TZ="Europe/London" date -v-mon +"%m/%d/%Y")
```

**Fallback Logic**:
```bash
START_DATE=$(TZ="Europe/London" date -v-mon +"%m/%d/%Y" 2>/dev/null || \
             TZ="Europe/London" date -d "monday" +"%m/%d/%Y")
```

### Timezone Handling

All workflows use **Europe/London** timezone:
- Consistent with business operations
- Handles BST/GMT transitions automatically
- GitHub Actions runs in UTC (automatically converted)

---

## File Locations

```
.github/workflows/
├── README.md                    # This documentation
├── run-scraper.yml              # Main workflow (enhanced)
├── yesterday-report.yml         # Yesterday's data (new)
└── week-to-date-report.yml      # Week-to-date (new)
```

---

## Next Steps

1. ✅ **Test Manual Triggers**:
   - Go to Actions tab
   - Try different date modes
   - Verify data is correct

2. ✅ **Monitor Scheduled Runs**:
   - Check runs appear on schedule
   - Verify artifacts are uploaded
   - Confirm Google Chat reports

3. ✅ **Customize Schedules** (optional):
   - Edit cron expressions for your needs
   - Add/remove specific times
   - Adjust for your timezone

4. ✅ **Review Artifacts**:
   - Download output files
   - Check data accuracy
   - Verify date ranges

---

## Migration Notes

### Before
- Only "today" data via main workflow
- Manual date ranges required custom config
- No automated historical reports

### After
- ✅ Multiple date modes via dropdown
- ✅ Automated yesterday reports
- ✅ Automated week-to-date reports
- ✅ Easy manual date selection
- ✅ No config changes needed

### Breaking Changes
**None!** All existing functionality preserved. New features are additive only.

---

**Implementation Date**: 2025-11-26
**Author**: Workflow Enhancement Feature
**Version**: 2.0
