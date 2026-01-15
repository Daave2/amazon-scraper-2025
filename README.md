# Amazon Seller Central Scraper


The scraper can run locally or through GitHub Actions with scheduled automation.

## Table of Contents

- [Key Features](#key-features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Local Setup](#local-setup)
- [Running Locally](#running-locally)
- [Date Range Selection](#date-range-selection)
- [GitHub Actions Workflow](#github-actions-workflow)
- [Configuration Reference](#configuration-reference)
- [INF Analysis](#inf-analysis)
- [Google Chat Reporting](#google-chat-reporting)
- [Testing](#testing)
- [Notes](#notes)

## Key Features

### 🤖 **Automated Data Collection**
- Collects key performance metrics for multiple stores from `urls.csv`
- Submits data to Google Forms for aggregation in Google Sheets
- **Flexible date filtering**: Today, Yesterday, Last 7/30 days, Week-to-Date, or custom ranges
- **Smart defaults**: Performance dashboard uses today's data; configurable via CLI or workflows

### 📱 **Rich Google Chat Reporting**
- Batch-grouped collapsible cards with alphabetized store listings
- **Performance Highlights**: Automatically reports Bottom 5 stores for:
  - 🕒 **Lates**: Highest percentage of late orders
  - ⚠️ **INF**: Highest Item Not Found rate  
  - 📦 **UPH**: Lowest Units Per Hour
- **Enhanced Job Summary** with:
  - Throughput metrics (orders/sec, success rate)
  - Business volume (total orders & units)
  - Performance breakdown (collection time, latency, bottlenecks)
  - Detailed failure analysis by error type
- **Smart Filtering**: Automatically excludes stores with 0 orders from reports
- **Enhanced INF Cards**:
  - High-resolution product images (300px)
  - QR codes for SKU lookup + clickable "Optics" button
  - Price and SKU display
  - Stock levels with last updated timestamp
  - Standard and promotional location details
  - Visual alerts for discontinued items
  - **Network-wide summary** showing top 3 contributing stores per problem item

### 🚀 **On-Demand Triggers**
- **Quick Actions buttons** in Google Chat for instant report generation
- Trigger any report on-demand via Apps Script integration
- **30-minute cooldown protection** prevents accidental duplicate runs
- **Instant acknowledgement** cards show who requested what
- **Simplified schedule**: 3 daily automated runs (8 AM, 12 PM, 2 PM)
- See [`docs/ON_DEMAND_TRIGGERS.md`](docs/ON_DEMAND_TRIGGERS.md) for setup and usage

### ⚙️ **Performance \u0026 Reliability**
- Dynamic concurrency adjustment based on CPU/memory load
- Concurrent browser workers with automatic scaling
- Parallel HTTP form submission
- Resource blocking for faster page loads (analytics, ads)
- Comprehensive error handling with retry logic
- Error-aware throttling during network issues

## Requirements

- **Python 3.11+**
- **Playwright** with Chromium browsers
- See `requirements.txt` for full Python package list

## Quick Start

```bash
# 1. Clone repository
git clone <your-repo-url>
cd amazon-scraper

# 2. Install dependencies
pip install -r requirements.txt
python -m playwright install chromium

# 3. Configure
cp config.example.json config.json
# Edit config.json with your credentials

# 4. Add store data
# Populate urls.csv with your stores

# 5. Run
python scraper.py
```

## Local Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 2. Configure Credentials

Copy the example configuration:

```bash
cp config.example.json config.json
```

Edit `config.json` with your credentials:

```json
{
  "login_email": "your@email.com",
  "login_password": "your_password",
  "otp_secret_key": "YOUR_OTP_SECRET",
  "form_url": "https://docs.google.com/forms/.../formResponse",
  "chat_webhook_url": "https://chat.googleapis.com/...",
}
```

See [Configuration Reference](#configuration-reference) for all options.

### 3. Prepare Store List

Populate `urls.csv` with your stores:

```csv
merchant_id,new_id,store_name,marketplace_id,store_number
A1234567,ABCDEF,Morrisons - Store Name,A1PA6795UKMFR9,1234
```

**Note**: `store_number` is required for stock enrichment (Morrisons API).

## Running Locally

### Default Mode (Today's Data)

```bash
python scraper.py
```

### With Date Range

```bash
# Today's data
python scraper.py --date-mode today

# Relative (e.g., yesterday)
python scraper.py --date-mode relative --relative-days -1

# Custom date range
python scraper.py --date-mode custom \
  --start-date "01/15/2025" \
  --end-date "01/20/2025" \
  --start-time "12:00 AM" \
  --end-time "11:59 PM"
```

Logs and data are saved in the `output/` directory.

## Date Range Selection

The scraper supports flexible date/time range selection with built-in presets and custom ranges:

### Modes

1. **Today** (`--date-mode today`)
   - Collects data for the current day (default)

2. **Yesterday** (`--date-mode yesterday`)
   - Previous day's data

3. **Last 7 Days** (`--date-mode last_7_days`)
   - Rolling 7-day window

4. **Last 30 Days** (`--date-mode last_30_days`)
   - Rolling 30-day window

5. **Week to Date** (`--date-mode week_to_date`)
   - Monday (start of week) to today

6. **Relative** (`--date-mode relative --relative-days N`)
   - Offset from today (e.g., `-1` for yesterday, `-7` for last week)

7. **Custom** (`--date-mode custom`)
   - Specify exact start/end dates and times
   - Useful for historical data analysis

### Configuration

In `config.json`:

```json
{
  "use_date_range": true,
  "date_range_mode": "yesterday",
  "custom_start_date": "01/15/2025",
  "custom_end_date": "01/20/2025",
  "custom_start_time": "12:00 AM",
  "custom_end_time": "11:59 PM"
}
```

Command-line arguments override config settings. See [`DATE_RANGE_FEATURE.md`](DATE_RANGE_FEATURE.md) for details.

## GitHub Actions Workflow

### Automated Scheduling

The project now uses specialized workflows for different report types:

**Daily Automated Reports:**
- **8 AM UK Time** (`yesterday-report.yml`) - Yesterday's full INF analysis
- **12 PM UK Time** (`midday-performance.yml`) - Current day performance check
- **2 PM UK Time** (`afternoon-inf.yml`) - Today's INF analysis (so far)

**On-Demand Reports:**
All other reports can be triggered via **Quick Actions buttons** in Google Chat:
- INF Analysis (Today) - Top 10 items per store
- Performance Check - Lates, UPH, and key metrics
- Yesterday's INF Report - Top 10 items from yesterday
- Week-to-Date INF - Monday through today summary

See [`docs/ON_DEMAND_TRIGGERS.md`](docs/ON_DEMAND_TRIGGERS.md) for setup and usage.

### Required Secrets

Configure these in **Settings → Secrets and variables → Actions**:


### Artifacts

- Logs and output files uploaded after each run
- Retained for 7 days

## Configuration Reference



### Performance Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `initial_concurrency` | int | 30 | Starting number of browser workers |
| `num_form_submitters` | int | 2 | Parallel HTTP form submitters |
| `page_timeout_ms` | int | 30000 | Page load timeout (ms) |
| `element_wait_timeout_ms` | int | 10000 | Element wait timeout (ms) |

### Auto-Concurrency

```json
{
  "auto_concurrency": {
    "enabled": true,
    "min_concurrency": 1,
    "max_concurrency": 55,
    "cpu_upper_threshold": 90,
    "cpu_lower_threshold": 65,
    "mem_upper_threshold": 90,
    "check_interval_seconds": 5,
    "cooldown_seconds": 15
  }
}
```

Automatically adjusts concurrency based on system load.

### Google Chat Integration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chat_webhook_url` | string | - | Google Chat webhook URL |
| `chat_batch_size` | int | 100 | Stores per chat card |

### Date Range Configuration

| Option | Type | Description |
|--------|------|-------------|
| `use_date_range` | boolean | Enable date range selection |
| `date_range_mode` | string | Mode: `today`, `relative`, or `custom` |
| `relative_days` | int | Offset for relative mode |
| `custom_start_date` | string | Start date (MM/DD/YYYY) |
| `custom_end_date` | string | End date (MM/DD/YYYY) |
| `custom_start_time` | string | Start time (e.g., "12:00 AM") |
| `custom_end_time` | string | End time (e.g., "11:59 PM") |

See `config.example.json` for the complete configuration schema.



### Configurable Depth

Control how many items to show per store:

```bash
# Show top 5 items (default)
python scraper.py --inf-only --top-n 5

# Show top 10 items
python scraper.py --inf-only --top-n 10

# Show top 25 items
python scraper.py --inf-only --top-n 25
```

Or via GitHub Actions workflow inputs:
- **Full INF Scrape** workflow has `top_items` dropdown (5, 10, 25)

**Note**: Batch sizes adjust automatically to prevent payload errors:
- Top 5: 8 stores/batch
- Top 10: 4 stores/batch
- Top 25: 3 stores/batch

### Google Chat Reporting

### Report Types

1. **Progress Updates** - Batched store results during run
2. **Job Summary** - Post-run statistics and metrics
3. **Performance Highlights** - Bottom 5 stores for Lates, INF, UPH
4. **INF Analysis** - Detailed reports with images and QR codes

### Features

- Collapsible card sections for easy scanning
- Emojis for visual indicators (✅ ❌ 📦 ⚠️ 📍)
- Color-coded metrics vs. targets
- Alphabetized store listings
- Smart filtering (excludes 0-order stores)

### INF Card Layout

```
┌────────────────────────────────────────┐
│ [Product Image - 300px]                │
│                                        │
│ 📦 SKU: 112571916                     │
│ 🔢 EAN: 5010525190303                 │
│ 💷 £2.50                               │
│ ⚠️ INF Units: 15                      │
│ 🚫 DISCONTINUED/NOT RANGED (if true)  │
│ 📊 Stock: 10 EA (at 14:30)            │
│ 📍 Aisle 5, Left bay 3, shelf 2       │
│ 🏷️ Aisle 95, Bay RE2, shelf 4        │
│                                        │
│ [QR Code]                              │
│ [🔍 Optics] (clickable button)        │
└────────────────────────────────────────┘
```

## Testing

Run the test suite:

```bash
pytest
```



### Timezone

All timestamps use `Europe/London` timezone by default. Modify `LOCAL_TIMEZONE` in `utils.py` to change.

### Browser State

- Authentication state cached in `state.json`
- Reused across runs to minimize logins
- Automatically re-authenticates if session expires

### Troubleshooting

Check logs in `app.log` and `output/` directory for:
- Login failures
- API authentication errors  
- Timeout issues
- Data extraction problems

#### Diagnostic Utilities


---

**Built using Playwright, Python, and modern async patterns**

