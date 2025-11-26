# Webhook Configuration

The scraper now supports separate webhook URLs for different report types, allowing you to send different reports to different Google Chat spaces.

## Configuration Fields

Add these fields to your `config.json`:

```json
{
  "chat_webhook_url": "https://chat.googleapis.com/v1/spaces/.../messages?key=...",
  "inf_webhook_url": "https://chat.googleapis.com/v1/spaces/.../messages?key=...",
  "store_webhook_url": "https://chat.googleapis.com/v1/spaces/.../messages?key=...",
  "performance_webhook_url": "https://chat.googleapis.com/v1/spaces/.../messages?key=..."
}
```

## Webhook Types

### 1. INF Webhook (`inf_webhook_url`)
**Used for**: INF (Item Not Found) Analysis Reports
- Network-wide top 10 INF items
- Per-store INF details with product images, QR codes, and stock information
- **Fallback**: Uses `chat_webhook_url` if not specified

### 2. Store Webhook (`store_webhook_url`)
**Used for**: Individual Store Metrics
- Store-by-store performance data (Orders, UPH, Lates%, INF%)
- Sent in batches during the main scraper run
- **Fallback**: Uses `chat_webhook_url` if not specified

### 3. Performance Webhook (`performance_webhook_url`)
**Used for**: Performance Summaries
- Job summary (completion status, throughput, success rate)
- Performance highlights (worst performing stores for Lates and UPH)
- **Fallback**: Uses `chat_webhook_url` if not specified

### 4. General Chat Webhook (`chat_webhook_url`)
**Used for**: Default/Fallback
- Acts as a fallback if specific webhooks are not configured
- If you only set this URL, all reports will go to the same space

## Setup Options

### Option 1: All Reports to One Space
Just set `chat_webhook_url`:
```json
{
  "chat_webhook_url": "https://chat.googleapis.com/..."
}
```

### Option 2: Separate Spaces for Each Report Type
Set all four URLs to different spaces:
```json
{
  "chat_webhook_url": "https://chat.googleapis.com/.../default-space",
  "inf_webhook_url": "https://chat.googleapis.com/.../inf-space",
  "store_webhook_url": "https://chat.googleapis.com/.../stores-space",
  "performance_webhook_url": "https://chat.googleapis.com/.../performance-space"
}
```

### Option 3: Mixed Configuration
Set some specific, let others fall back:
```json
{
  "chat_webhook_url": "https://chat.googleapis.com/.../general-reports",
  "inf_webhook_url": "https://chat.googleapis.com/.../inf-only-space"
}
```
In this case:
- INF reports → `inf-only-space`
- Store details → `general-reports` (fallback)
- Performance summaries → `general-reports` (fallback)

## How to Get Webhook URLs

1. Open Google Chat
2. Go to the space where you want to receive reports
3. Click the space name → **Apps & integrations**
4. Click **Add webhooks**
5. Name the webhook (e.g., "INF Reports")
6. Click **Save**
7. Copy the webhook URL
8. Paste it into your `config.json`

## Testing

After updating your `config.json`, run the scraper and verify that reports appear in the expected spaces:

```bash
python scraper.py --date-mode today
```

Check each Google Chat space to confirm the reports arrived correctly.
