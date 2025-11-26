# Date Range Time Fix - Summary

## Issues Identified

### Issue #1: Partial Day Coverage
When running "yesterday" mode at 6 AM, the scraper was only collecting data up to 6 AM of yesterday, not the **full 24 hours** of yesterday (12:00 AM to 11:59 PM).

**Root Cause**: The `--date-mode relative --relative-days -1` command wasn't explicitly specifying start and end times, so it defaulted to current time.

### Issue #2: INF Analysis Ignored Date Range
INF deep-dive analysis was always using "today's" data, even when the main scraper was run with yesterday or custom date ranges.

**Root Cause**: INF scraper (`inf_scraper.py`) wasn't importing or applying date range logic from `date_range.py`.

---

## Fixes Applied

### Fix #1: Explicit Time Ranges for All Date Modes

Updated all workflows and commands to always specify **full 24-hour periods**:

#### Workflows Updated
1. **`run-scraper.yml`** - Main workflow manual triggers
2. **`yesterday-report.yml`** - Yesterday  automated workflow

#### Changes Made

**Before**:
```bash
python scraper.py --date-mode relative --relative-days -1
```

**After**:
```bash
python scraper.py --date-mode relative --relative-days -1 \
  --start-time "12:00 AM" --end-time "11:59 PM"
```

**Applied to**:
- ✅ `yesterday` mode → Full 24 hours of previous day
- ✅ `week_to_date` mode → Full days from Monday through today
- ✅ `relative` mode → Full 24 hours of offset day
- ✅ `custom` mode → Full days for specified range

**Unchanged**:
- ⏱️ `today` mode → Still uses current time (as expected for real-time data)

---

### Fix #2: INF Analysis Now Respects Date Range

Updated `inf_scraper.py` to apply the same date range used by the main scraper:

#### Files Modified
- **`inf_scraper.py`**

#### Changes Made

1. **Added Date Range Imports**:
   ```python
   from date_range import get_date_time_range_from_config, apply_date_time_range
   ```

2. **Updated `process_store_task` Function**:
   - Added `date_range_func` and `action_timeout` parameters
   - Applies date range after navigating to INF page
   - Logs whether date range was applied successfully

   ```python
   async def process_store_task(..., date_range_func=None, action_timeout=20000):
       ...
       await page.goto(inf_url, ...)
       
       # Apply date range if configured (same as main scraper)
       if date_range_func:
           date_range_applied = await apply_date_time_range(
               page, store_name, date_range_func, action_timeout, DEBUG_MODE, app_logger
           )
       ...
   ```

3. **Updated `worker` Function**:
   - Added `date_range_func` and `action_timeout` parameters
   - Passes them to `process_store_task`

4. **Updated `run_inf_analysis` Function**:
   - Creates `get_date_range` function from config
   - Calculates `ACTION_TIMEOUT`
   - Passes both to workers

   ```python
   def get_date_range():
       return get_date_time_range_from_config(config, LOCAL_TIMEZONE, app_logger)
   
   ACTION_TIMEOUT = int(PAGE_TIMEOUT / 2)
   
   workers = [
       asyncio.create_task(worker(..., get_date_range, ACTION_TIMEOUT))
       for i in range(num_workers)
   ]
   ```

---

## Behavior Changes

### Before Fixes

| Scenario | Main Scraper | INF Analysis |
|----------|--------------|--------------|
| Run at 6 AM with "yesterday" | Yesterday 00:00 - 06:00 ❌ | Today's data ❌ |
| Run at 2 PM with "yesterday" | Yesterday 00:00 - 14:00 ❌ | Today's data ❌ |
| Run with custom range | Specified range ✅ | Today's data ❌ |

### After Fixes

| Scenario | Main Scraper | INF Analysis |
|----------|--------------|--------------|
| Run at 6 AM with "yesterday" | Yesterday 00:00 - 23:59 ✅ | Yesterday 00:00 - 23:59 ✅ |
| Run at 2 PM with "yesterday" | Yesterday 00:00 - 23:59 ✅ | Yesterday 00:00 - 23:59 ✅ |
| Run with custom range | Specified range ✅ | Specified range ✅ |

---

## Example Usage

### Yesterday's Full Day Data

**Command**:
```bash
python scraper.py --date-mode relative --relative-days -1 \
  --start-time "12:00 AM" --end-time "11:59 PM"
```

**Result**:
- ✅ Main scraper collects ALL of yesterday (12:00 AM to 11:59 PM)
- ✅ INF analysis uses same date range (yesterday's full day)
- ✅ Reports show complete 24-hour data

### Week-to-Date

**Command** (calculated automatically):
```bash
python scraper.py --date-mode custom \
  --start-date "01/20/2025" \  # Monday
  --end-date "01/24/2025" \    # Today (Friday)
  --start-time "12:00 AM" \
  --end-time "11:59 PM"
```

**Result**:
- ✅ Main scraper collects Monday 00:00 through Friday 23:59
- ✅ INF analysis uses same range
- ✅ Full week's data aggregated

---

## Testing Verification

To verify the fixes are working:

### 1. Check Logs for Date Range Application

**Main Scraper**:
```
[Store Name] Applying date/time range: 01/25/2025 12:00 AM to 01/25/2025 11:59 PM
```

**INF Analysis**:
```
[Store Name] Date range applied to INF page
```

### 2. Verify Data Coverage

- Check that orders/units match expected full-day totals
- Verify INF items are from the correct date range
- Confirm timestamps in output match specified range

### 3. Test Different Modes

```bash
# Yesterday (full 24 hours)
python scraper.py --date-mode relative --relative-days -1 --start-time "12:00 AM" --end-time "11:59 PM"

# Week-to-date (Monday through today, all full days)
python scraper.py --date-mode custom --start-date "01/20/2025" --end-date "01/24/2025" --start-time "12:00 AM" --end-time "11:59 PM"

# Specific day (full 24 hours)
python scraper.py --date-mode custom --start-date "01/15/2025" --end-date "01/15/2025" --start-time "12:00 AM" --end-time "11:59 PM"
```

---

## Technical Details

### Date Range Application Flow

1. **Config/CLI args** set date mode and times
2. **`get_date_time_range_from_config()`** calculates the range
3. **Main scraper** applies range to each store's dashboard
4. **INF scraper** (if triggered) uses the **same config**
5. **Date range applied** to INF page before data extraction

### Time Zone Handling

All dates/times use **Europe/London** timezone:
- Consistent across main scraper and INF analysis
- Automated date calculations (e.g., "Monday of this week") use London time
- GitHub Actions converts from UTC automatically

---

## Files Modified

1. ✅ **`.github/workflows/run-scraper.yml`** - Added explicit times to all date modes
2. ✅ **`.github/workflows/yesterday-report.yml`** - Added full day time range
3. ✅ **`inf_scraper.py`** - Added date range support (imports, functions, workers)

---

## Breaking Changes

**None!** 

- Existing functionality preserved
- Default behavior unchanged (today mode still uses current time)
- Only fixes incorrect partial-day behavior

---

**Fixed**: 2025-11-26  
**Impact**: High - Ensures data completeness and consistency  
**Testing**: Required before production use
