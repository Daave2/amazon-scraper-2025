# Stock Enrichment Feature

The INF scraper can optionally enrich the extracted INF data with real-time stock levels and location information from the Morrisons API.

## Configuration

Add the following to your `config.json`:

```json
{
  "morrisons_api_key": "YOUR_MORRISONS_API_KEY",
  "morrisons_bearer_token": "YOUR_BEARER_TOKEN_OPTIONAL",
  "enrich_stock_data": true
}
```

### Configuration Options

- **`morrisons_api_key`** (required for enrichment): Your Morrisons API key
- **`morrisons_bearer_token`** (optional): Bearer token for authentication. If authentication fails, the system will retry without it.
- **`enrich_stock_data`** (boolean): Set to `true` to enable stock enrichment, `false` to disable

## What It Does

When enabled, for each INF item extracted from Amazon Seller Central, the scraper will:

1. Query the Morrisons Product API to get product details
2. Query the Morrisons Stock API to get current stock levels
3. Query the Morrisons Price Integrity API to get shelf locations

The enriched data includes:
- **Stock on hand**: Current quantity in stock
- **Stock unit**: Unit of measure (e.g., "CASES", "EACH")
- **Standard location**: Regular shelf location (e.g., "Aisle 12, Left bay 3, shelf 2")
- **Promotional location**: Promotional display location if applicable

## Report Format

Items with enriched data will display as:

```
**25 - Coca-Cola Zero Sugar 24x330ml**
    📦 Stock: 15 CASES
    📍 Aisle 12, Left bay 3, shelf 2
```

Items without stock data (API not configured or data unavailable) will display as:

```
25 - Coca-Cola Zero Sugar 24x330ml
```

## Performance Notes

- Stock enrichment makes 2-3 API calls per item per store
- For 10 items per store across 100 stores, this is ~2,000-3,000 API calls
- Calls are made concurrently using `asyncio.to_thread()` for efficiency
- Total enrichment time adds approximately 30-60 seconds to the scraper run

## Troubleshooting

If enrichment fails for specific items, check the logs for:
- `Product {sku} not found in Morrisons API` - SKU doesn't exist in the API
- `HTTP error for {url}` - API authentication or connectivity issue
- `Found stock for SKU {sku}` - Successful stock lookup

The scraper will continue even if enrichment fails for some items.
