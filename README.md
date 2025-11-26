# Amazon Seller Central Scraper

An advanced, asynchronous scraper built with Playwright that automates data collection from Amazon Seller Central. It extracts dashboard metrics for multiple stores, performs deep-dive INF (Item Not Found) analysis, enriches data with real-time stock information, and delivers intelligent reports via Google Chat.

The scraper can run locally or through GitHub Actions with scheduled automation.

https://github.com/user-attachments/assets/7ba7f0f6-4d0b-4cc2-9937-de5ad766fea4

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
- [Stock Enrichment](#stock-enrichment)
- [Google Chat Reporting](#google-chat-reporting)
- [Testing](#testing)
- [Notes](#notes)

## Key Features

### 🤖 **Automated Data Collection**
- Automates Amazon Seller Central sign-in with two-factor authentication (OTP)
- Collects key performance metrics for multiple stores from `urls.csv`
- Submits data to Google Forms for aggregation in Google Sheets
- Supports configurable date/time range selection (today, relative, custom)

### 📊 **Intelligent INF Analysis**
- Automatically identifies bottom 10 stores by INF rate for deep-dive analysis
- Extracts top 10 problematic items per store from INF dashboard
- Displays product images, SKU details, and INF occurrence counts
- **NEW**: Enhanced reports with higher resolution images and QR codes

### 🏪 **Morrisons Stock Integration** 
- Real-time stock level lookup via Morrisons API
- Shelf location information (aisle, bay, shelf)
- Supports multi-component products with fallback SKU logic
- Bearer token authentication with automatic refresh from GitHub Gist

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
- **Enhanced INF Cards**: Higher resolution product images (300px), QR codes for SKU lookup, formatted text with emojis

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
  "morrisons_api_key": "YOUR_API_KEY",
  "morrisons_bearer_token_url": "https://gist.githubusercontent.com/.../raw/...",
  "enrich_stock_data": true
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

The scraper supports flexible date/time range selection:

### Modes

1. **Today** (`--date-mode today`)
   - Collects data for the current day
   - Default behavior

2. **Relative** (`--date-mode relative --relative-days N`)
   - Offset from today (e.g., `-1` for yesterday, `-7` for last week)

3. **Custom** (`--date-mode custom`)
   - Specify exact start/end dates and times
   - Useful for historical data analysis

### Configuration

In `config.json`:

```json
{
  "use_date_range": true,
  "date_range_mode": "custom",
  "custom_start_date": "01/15/2025",
  "custom_end_date": "01/20/2025",
  "custom_start_time": "12:00 AM",
  "custom_end_time": "11:59 PM"
}
```

Command-line arguments override config settings. See [`DATE_RANGE_FEATURE.md`](DATE_RANGE_FEATURE.md) for details.

## GitHub Actions Workflow

### Automated Scheduling

The workflow in `.github/workflows/run-scraper.yml`:

- Runs every hour via cron schedule
- Checks current UK time against `UK_TARGET_HOURS`
- Only executes at specified times (e.g., 09:00, 12:00, 14:00, 17:00, 20:00)
- Can be triggered manually via "Actions" tab

### Required Secrets

Configure these in **Settings → Secrets and variables → Actions**:

- `FORM_URL` - Google Form submission URL
- `LOGIN_URL` - Amazon Seller Central login URL
- `SECRET_KEY` - Encryption key for sensitive data
- `LOGIN_EMAIL` - Your Amazon account email
- `LOGIN_PASSWORD` - Your Amazon account password
- `OTP_SECRET_KEY` - TOTP secret for 2FA
- `CHAT_WEBHOOK_URL` - Google Chat webhook URL
- `MORRISONS_API_KEY` - Morrisons API key
- `MORRISONS_BEARER_TOKEN_URL` - URL to fetch Morrisons bearer token (e.g., GitHub Gist)

### Artifacts

- Logs and output files uploaded after each run
- Retained for 7 days
- Authentication state cached between runs

## Configuration Reference

### Core Settings

| Option | Type | Description |
|--------|------|-------------|
| `login_email` | string | Amazon Seller Central email |
| `login_password` | string | Amazon Seller Central password |
| `otp_secret_key` | string | TOTP secret for 2FA |
| `form_url` | string | Google Form submission URL |
| `debug` | boolean | Enable verbose logging and screenshots |

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

### Morrisons API (Stock Enrichment)

| Option | Type | Description |
|--------|------|-------------|
| `morrisons_api_key` | string | Morrisons API key |
| `morrisons_bearer_token_url` | string | URL to fetch bearer token |
| `enrich_stock_data` | boolean | Enable/disable stock enrichment |

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

## INF Analysis

### Automatic Triggering

After main scraper completes, the system:
1. Identifies bottom 10 stores by INF rate
2. Automatically launches deep-dive analysis
3. Extracts top 10 problematic items per store
4. Sends detailed reports to Google Chat

### Enhanced Reports

Each INF item card displays:
- **Product image** (300px high-resolution)
- **Product name** (bold)
- **SKU** (color-coded in blue)
- **INF occurrence count** (bold)
- **Stock level** (if enrichment enabled)
- **Shelf location** (if available)
- **QR code** (scannable SKU for warehouse staff)

### Manual Execution

```bash
python inf_scraper.py
```

See [`docs/INF_REPORT_ENHANCEMENTS.md`](docs/INF_REPORT_ENHANCEMENTS.md) for layout details.

## Stock Enrichment

When enabled, the scraper enriches INF data with:

- **Stock on hand**: Current quantity (e.g., "15 CASES")
- **Standard location**: Shelf location (e.g., "Aisle 5, Left bay 3, shelf 2")
- **Promotional location**: Promo display location (if applicable)

### Requirements

1. Morrisons API credentials configured
2. Store numbers populated in `urls.csv`
3. `enrich_stock_data` set to `true` in config

### Performance

- 2-3 API calls per item
- Concurrent execution via `asyncio`
- Adds ~30-60 seconds to scraper run

See [`STOCK_ENRICHMENT.md`](STOCK_ENRICHMENT.md) for details.

## Google Chat Reporting

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

### Message Layout

```
┌────────────────────────────────────────┐
│ [Product Image - 300px]                │
│                                        │
│ 📦 SKU: 112571916                     │
│ ⚠️ INF Units: 15                      │
│ 📊 Stock: 10 CASES                    │
│ 📍 Aisle 5, Left bay 3, shelf 2       │
│                                        │
│ [QR Code]                              │
│ Scan to lookup SKU                     │
└────────────────────────────────────────┘
```

## Testing

Run the test suite:

```bash
pytest
```

This executes tests for:
- Webhook formatting and filtering
- Date range calculations
- Stock enrichment API calls

### Diagnostic Scripts

Located in `utils/` directory:

- `utils/test_morrisons_api.py` - Comprehensive Morrisons API diagnostics
- `utils/test_morrisons_config.py` - Quick configuration validation

See [`utils/README.md`](utils/README.md) for usage details.

## Notes

### Security

- **Never commit** `config.json`, `state.json`, or `output/` to version control
- Use GitHub Secrets for CI/CD credentials
- Rotate API keys and tokens regularly

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

Use the scripts in `utils/` to diagnose issues:

```bash
# Check Morrisons API configuration
python3 utils/test_morrisons_config.py

# Test Morrisons API connectivity and authentication
python3 utils/test_morrisons_api.py
```

See [`utils/README.md`](utils/README.md) for detailed usage and troubleshooting workflows.

For Morrisons API issues, see [`docs/MORRISONS_API_FIX.md`](docs/MORRISONS_API_FIX.md).

---

**Built with ❤️ using Playwright, Python, and modern async patterns**

