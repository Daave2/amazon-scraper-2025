import asyncio
import re
from typing import Any, Dict, List
import requests
from utils import setup_logging

app_logger = setup_logging()

BASE_PRODUCT = "https://api.morrisons.com/product/v1/items"
BASE_STOCK = "https://api.morrisons.com/stock/v2/locations"
BASE_LOCN = "https://api.morrisons.com/priceintegrity/v1/locations"

HEADERS_BASE = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (INF Scraper-StockChecker)",
}

_SIDE_RE = re.compile(r"^([LR])(\d+)$", re.I)


def fetch_bearer_token_from_gist(gist_url: str) -> str | None:
    """Fetch the bearer token from a GitHub gist URL."""
    try:
        response = requests.get(gist_url, timeout=10)
        response.raise_for_status()
        token = response.text.strip()
        app_logger.info("Successfully fetched bearer token from gist")
        return token
    except Exception as e:
        app_logger.warning(f"Failed to fetch bearer token from gist: {e}")
        return None


def _http_get(url: str, bearer: str | None) -> requests.Response:
    """Performs a single synchronous HTTP GET request."""
    h = HEADERS_BASE.copy()
    if bearer:
        h["Authorization"] = f"Bearer {bearer}"
    return requests.get(url, headers=h, timeout=15)


def _fetch_json(url: str, bearer: str | None) -> Dict[str, Any] | None:
    """Fetches and parses JSON from a URL, with a retry for auth failure."""
    try:
        r = _http_get(url, bearer)
        if r.status_code in (401, 403) and bearer:
            app_logger.debug(f"Bearer token failed for {url}; retrying without it.")
            r = _http_get(url, None)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        if e.response and e.response.status_code == 404:
            return None  # Return None for 404s to distinguish from other errors
        app_logger.warning(f"HTTP error for {url}: {e}")
        return None
    except Exception as e:
        app_logger.warning(f"Error fetching {url}: {e}")
        return None


# --- Location Formatting Helpers ---
def nice_loc(raw: Dict) -> str:
    aisle = raw.get("aisle", "")
    bay = raw.get("bayNumber", "")
    shelf = raw.get("shelfNumber", "")
    side = ""
    m = _SIDE_RE.match(bay)
    if m:
        side = "Left" if m.group(1).upper() == "L" else "Right"
        bay = m.group(2)
    parts = []
    if aisle:
        parts.append(f"Aisle {aisle}")
    if side:
        parts.append(f"{side} bay {bay}")
    elif bay:
        parts.append(f"Bay {bay}")
    if shelf:
        parts.append(f"shelf {shelf}")
    return ", ".join(parts)


def simplify_locations(lst: List[Dict]) -> str:
    return "; ".join(nice_loc(l) for l in lst) if lst else ""


def extract_location_bits(pi: Dict | None) -> tuple[str, str, str | None]:
    if not pi:
        return "", "", None
    space = pi.get("space", {})
    std_lst = space.get("standardSpace", {}).get("locations", [])
    promo_lst = space.get("promotionalSpace", {}).get("locations", [])
    aisle_number = std_lst[0].get("aisle") if std_lst else None
    return simplify_locations(std_lst), simplify_locations(promo_lst), aisle_number


def _fetch_morrisons_data_for_sku(sku: str, location_id: str, api_key: str, bearer_token: str | None) -> Dict[str, Any]:
    """
    Synchronous worker to fetch product, stock, and location data for a SKU.
    Designed to be run in a separate thread.
    """
    try:
        # 1. Get product details to find all possible component SKUs
        product_url = f"{BASE_PRODUCT}/{sku}?apikey={api_key}"
        product_data = _fetch_json(product_url, bearer_token)
        if not product_data:
            app_logger.debug(f"Product {sku} not found in Morrisons API.")
            return {}

        # 2. Collect all candidate SKUs (primary + pack components)
        candidate_skus = [sku] + [
            str(pc["itemNumber"])
            for pc in product_data.get("packComponents", [])
            if pc.get("itemNumber")
        ]

        # 3. Try each SKU to find a stock record
        stock_sku_found, stock_payload = None, None
        for s in candidate_skus:
            stock_url = f"{BASE_STOCK}/{location_id}/items/{s}?apikey={api_key}"
            payload = _fetch_json(stock_url, bearer_token)
            if payload:
                stock_sku_found = s
                stock_payload = payload
                break

        # 4. Extract stock and location information
        results = {}
        if stock_payload:
            pos = (stock_payload or {}).get("stockPosition", [{}])[0]
            results["stock_on_hand"] = pos.get("qty")
            results["stock_unit"] = pos.get("unitofMeasure")
            results["stock_last_updated"] = pos.get("lastUpdated")
            app_logger.debug(
                f"Found stock for SKU {stock_sku_found} (original {sku}): {pos.get('qty')}"
            )

        # 5. Fetch Price Integrity (location) using the SKU that had stock
        pi_sku = stock_sku_found or sku  # Fallback to original SKU
        pi_url = f"{BASE_LOCN}/{location_id}/items/{pi_sku}?apikey={api_key}"
        pi_data = _fetch_json(pi_url, bearer_token)
        if pi_data:
            std_loc, promo_loc, aisle_number = extract_location_bits(pi_data)
            results["std_location"] = std_loc
            results["promo_location"] = promo_loc
            results["aisle_number"] = aisle_number
            app_logger.debug(f"Found locations for PI SKU {pi_sku}")

        return results

    except Exception as e:
        app_logger.error(f"Unexpected error fetching data for {sku}: {e}")
        return {}


async def enrich_items_with_stock_data(items: List[Dict], location_id: str, api_key: str, bearer_token: str | None = None) -> List[Dict]:
    """
    Takes a list of scraped items and adds Morrisons stock and location data.
    """
    if not all([api_key, location_id]):
        app_logger.warning("Morrisons API settings missing, skipping enrichment.")
        return items

    if not items:
        return items

    # Create a list of tasks to run the blocking I/O in parallel threads
    tasks = [
        asyncio.to_thread(_fetch_morrisons_data_for_sku, item["sku"], location_id, api_key, bearer_token) 
        for item in items
    ]

    app_logger.info(f"Fetching stock & location data for {len(tasks)} items...")
    morrisons_results = await asyncio.gather(*tasks)

    # Merge original item data with the new data
    enriched_items = [
        {**original_item, **morrisons_data}
        for original_item, morrisons_data in zip(items, morrisons_results)
    ]

    app_logger.info("Finished enriching items with Morrisons data.")
    return enriched_items
