
import logging
import json
import asyncio
import re
import os
import csv
from datetime import datetime
from typing import List, Dict
from asyncio import Queue, Lock, Condition
from playwright.async_api import async_playwright, Page, TimeoutError, expect, Browser

# Import modules
from utils import setup_logging, sanitize_store_name, _save_screenshot, load_default_data, ensure_storage_state, LOCAL_TIMEZONE
from auth import check_if_login_needed, perform_login_and_otp, prime_master_session
from workers import auto_concurrency_manager
from stock_enrichment import enrich_items_with_stock_data

# Setup logging
app_logger = setup_logging()

# Config
try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    app_logger.critical("config.json not found.")
    exit(1)

DEBUG_MODE = config.get('debug', False)
LOGIN_URL = config['login_url']
CHAT_WEBHOOK_URL = config.get('chat_webhook_url')
STORAGE_STATE = 'state.json'
OUTPUT_DIR = 'output'
PAGE_TIMEOUT = config.get('page_timeout_ms', 30000)
STORE_PREFIX_RE = re.compile(r"^morrisons\s*-\s*", re.I)

# Morrisons API Config
MORRISONS_API_KEY = config.get('morrisons_api_key')
MORRISONS_BEARER_TOKEN_URL = config.get('morrisons_bearer_token_url', 
    "https://gist.githubusercontent.com/Daave2/b62faeed0dd435100773d4de775ff52d/raw/gistfile1.txt")
ENRICH_STOCK_DATA = config.get('enrich_stock_data', True)  # Enabled by default

# Fetch bearer token from gist at startup
MORRISONS_BEARER_TOKEN = None
if ENRICH_STOCK_DATA and MORRISONS_BEARER_TOKEN_URL:
    from stock_enrichment import fetch_bearer_token_from_gist
    MORRISONS_BEARER_TOKEN = fetch_bearer_token_from_gist(MORRISONS_BEARER_TOKEN_URL)

# Concurrency Config
INITIAL_CONCURRENCY = config.get('initial_concurrency', 2) # Start lower to be safe
AUTO_CONF = config.get('auto_concurrency', {})
AUTO_ENABLED = AUTO_CONF.get('enabled', True) 
AUTO_MIN_CONCURRENCY = AUTO_CONF.get('min_concurrency', 1)
AUTO_MAX_CONCURRENCY = AUTO_CONF.get('max_concurrency', 20)
CPU_UPPER_THRESHOLD = AUTO_CONF.get('cpu_upper_threshold', 90)
CPU_LOWER_THRESHOLD = AUTO_CONF.get('cpu_lower_threshold', 65)
MEM_UPPER_THRESHOLD = AUTO_CONF.get('mem_upper_threshold', 90)
CHECK_INTERVAL = AUTO_CONF.get('check_interval_seconds', 5)
COOLDOWN_SECONDS = AUTO_CONF.get('cooldown_seconds', 15)

INF_PAGE_URL = "https://sellercentral.amazon.co.uk/snow-inventory/inventoryinsights/ref=xx_infr_dnav_xx"

async def navigate_and_extract_inf(page: Page, store_name: str):
    """Extract INF data from the already-loaded INF page"""
    app_logger.info(f"Extracting INF data for: {store_name}")
    
    try:
        # Define table selector
        table_sel = "table.imp-table tbody"
        
        # Wait for table rows to appear
        try:
            await expect(page.locator(f"{table_sel} tr").first).to_be_visible(timeout=20000)
        except TimeoutError:
            app_logger.info(f"[{store_name}] No data rows found; returning empty list.")
            return []
        
        # Sort by INF Occurrences (only thing we need!)
        try:
            inf_sort = page.get_by_role("link", name="INF Occurrences")
            await inf_sort.click()
            await page.wait_for_timeout(2000)  # Wait for table to sort
            app_logger.info(f"[{store_name}] Sorted by INF Occurrences")
        except Exception as e:
            app_logger.warning(f"[{store_name}] Failed to sort: {e}")

        # Extract Data - just the top 10 rows
        rows = await page.locator(f"{table_sel} tr").all()
        app_logger.info(f"[{store_name}] Found {len(rows)} rows; extracting top 10")
        
        extracted_data = []
        
        for i, row in enumerate(rows[:10]):
            try:
                cells = row.locator("td")
                
                # Columns: image(0), sku(1), product_name(2), inf_units(3), etc.
                sku = await cells.nth(1).locator("span").inner_text()
                product_name = await cells.nth(2).locator("a span").inner_text()
                inf_units = await cells.nth(3).locator("span").inner_text()
                
                # Clean up
                sku = sku.strip()
                product_name = product_name.strip()
                inf_value = re.sub(r'[^\d]', '', inf_units)
                inf_value = int(inf_value) if inf_value else 0
                
                extracted_data.append({
                    "store": store_name,
                    "sku": sku,
                    "name": product_name,
                    "inf": inf_value
                })
            except Exception as e:
                app_logger.warning(f"[{store_name}] Error extracting row {i}: {e}")
        
        app_logger.info(f"[{store_name}] Extracted {len(extracted_data)} items.")
        return extracted_data

    except Exception as e:
        app_logger.error(f"[{store_name}] Error processing INF page: {e}")
        await _save_screenshot(page, f"error_inf_{sanitize_store_name(store_name, STORE_PREFIX_RE)}", OUTPUT_DIR, LOCAL_TIMEZONE, app_logger)
        return []

async def process_store_task(context, store_info, results_list, results_lock, failure_lock, failure_timestamps):
    merchant_id = store_info['merchant_id']
    marketplace_id = store_info['marketplace_id']
    store_name = store_info['store_name']
    store_number = store_info.get('store_number', '')
    
    page = None
    try:
        page = await context.new_page()
        
        # Navigate directly to INF page with store context (like reference script)
        inf_url = (
            "https://sellercentral.amazon.co.uk/snow-inventory/inventoryinsights/"
            f"?ref_=mp_home_logo_xx&cor=mmp_EU"
            f"&mons_sel_dir_mcid={merchant_id}"
            f"&mons_sel_mkid={marketplace_id}"
        )
        
        await page.goto(inf_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        
        # Now extract INF data
        items = await navigate_and_extract_inf(page, store_name)
        
        # Enrich with stock data if enabled and we have a store number
        if ENRICH_STOCK_DATA and store_number and items and MORRISONS_API_KEY:
            try:
                app_logger.info(f"[{store_name}] Enriching {len(items)} items with stock data...")
                items = await enrich_items_with_stock_data(
                    items, 
                    store_number, 
                    MORRISONS_API_KEY, 
                    MORRISONS_BEARER_TOKEN
                )
            except Exception as e:
                app_logger.warning(f"[{store_name}] Failed to enrich with stock data: {e}")
        
        async with results_lock:
            results_list.append((store_name, store_number, items))
            
    except Exception as e:
        app_logger.error(f"Failed to process {store_name}: {e}")
        async with failure_lock:
            failure_timestamps.append(asyncio.get_event_loop().time())
    finally:
        if page:
            try:
                await page.close()
            except:
                pass

async def worker(worker_id: int, browser: Browser, storage_state: Dict, job_queue: Queue, 
                 results_list: List, results_lock: Lock,
                 concurrency_limit_ref: dict, active_workers_ref: dict, concurrency_condition: Condition,
                 failure_lock: Lock, failure_timestamps: List):
    
    app_logger.info(f"[Worker-{worker_id}] Starting...")
    context = None
    try:
        context = await browser.new_context(storage_state=storage_state)
        # Block resources to speed up
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in ("image", "stylesheet", "font", "media") else route.continue_())
        
        while True:
            try:
                store_info = job_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            # Enforce Concurrency Limit
            async with concurrency_condition:
                while active_workers_ref['value'] >= concurrency_limit_ref['value']:
                    await concurrency_condition.wait()
                active_workers_ref['value'] += 1
            
            try:
                await process_store_task(context, store_info, results_list, results_lock, failure_lock, failure_timestamps)
            except Exception as e:
                app_logger.error(f"[Worker-{worker_id}] Error processing store: {e}")
            finally:
                async with concurrency_condition:
                    active_workers_ref['value'] -= 1
                    concurrency_condition.notify_all()
                job_queue.task_done()
    except Exception as e:
        app_logger.error(f"[Worker-{worker_id}] Crashed: {e}")
    finally:
        if context:
            try:
                await context.close()
            except:
                pass
        app_logger.info(f"[Worker-{worker_id}] Finished.")

async def send_inf_report(store_data: List[tuple], network_top_10: List):
    """
    Send INF report to Google Chat.
    store_data is a list of tuples: (store_name, store_number, items)
    """
    import aiohttp
    import ssl
    import certifi
    
    if not CHAT_WEBHOOK_URL:
        app_logger.warning("No Chat Webhook URL configured.")
        return

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    timeout = aiohttp.ClientTimeout(total=30)
    
    # Message 1: Network Wide Top 10
    sections_network = []
    widgets_network = []
    widgets_network.append({"textParagraph": {"text": "<b>🏆 Top 10 Network Wide (INF Occurrences)</b>"}})
    
    for item in network_top_10:
        text = f"<b>{item['inf']}</b> - {item['name']}"
        widgets_network.append({"textParagraph": {"text": text}})
        
    sections_network.append({"widgets": widgets_network})
    
    payload_network = {
        "cardsV2": [{
            "cardId": f"inf-network-{int(datetime.now().timestamp())}",
            "card": {
                "header": {
                    "title": "INF Analysis - Network Wide",
                    "subtitle": datetime.now(LOCAL_TIMEZONE).strftime("%A %d %B, %H:%M"),
                    "imageUrl": "https://cdn-icons-png.flaticon.com/512/272/272525.png",
                    "imageType": "CIRCLE"
                },
                "sections": sections_network,
            },
        }]
    }
    
    # Send network-wide report
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(CHAT_WEBHOOK_URL, json=payload_network) as resp:
                if resp.status != 200:
                    app_logger.error(f"Failed to send network INF report: {await resp.text()}")
                else:
                    app_logger.info("Network-wide INF report sent successfully.")
    except Exception as e:
        app_logger.error(f"Error sending network INF report: {e}")
        return
    
    # Message 2+: All Stores (split into batches to avoid size limits)
    # Sort by store name
    sorted_store_data = sorted(store_data, key=lambda x: x[0])
    stores_with_data = [(name, num, items) for name, num, items in sorted_store_data if items]
    
    # Batch size set to 25 as we are limiting to top 5 items and removing location data
    BATCH_SIZE = 25
    batches = [stores_with_data[i:i + BATCH_SIZE] for i in range(0, len(stores_with_data), BATCH_SIZE)]
    
    for batch_num, batch in enumerate(batches, 1):
        sections_stores = []
        
        for store_name, store_number, items in batch:
            widgets_store = []
            clean_store_name = sanitize_store_name(store_name, STORE_PREFIX_RE)
            total_inf = sum(item['inf'] for item in items)
            
            # Add store number if available
            if store_number:
                header_text = f"<b>#{store_number} {clean_store_name}</b> (Total: {total_inf})"
            else:
                header_text = f"<b>{clean_store_name}</b> (Total: {total_inf})"
                
            widgets_store.append({"textParagraph": {"text": header_text}})
            
            # Show top 5 items with stock data if available (Location removed per request)
            for item in items[:5]:
                # Build item text with INF count and product name
                item_parts = [f"{item['inf']} - {item['name']}"]
                
                # Add stock info if available
                if item.get('stock_on_hand') is not None:
                    stock_qty = item.get('stock_on_hand', 0)
                    stock_unit = item.get('stock_unit', '')
                    item_parts.append(f"📦 Stock: {stock_qty} {stock_unit}".strip())
                
                # Format the full item text
                if len(item_parts) > 1:
                    # Multi-line format for enriched data
                    main_text = item_parts[0]
                    widgets_store.append({"textParagraph": {"text": f"  <b>{main_text}</b>"}})
                    
                    # Add stock as sub-items
                    for extra in item_parts[1:]:
                        widgets_store.append({"textParagraph": {"text": f"    {extra}"}})
                else:
                    # Simple one-line format
                    widgets_store.append({"textParagraph": {"text": f"  {item_parts[0]}"}})
                 
            # Use store number in header if available
            section_header = f"#{store_number} {clean_store_name}" if store_number else clean_store_name
            sections_stores.append({"header": section_header, "collapsible": True, "widgets": widgets_store})
        
        payload_stores = {
            "cardsV2": [{
                "cardId": f"inf-stores-{batch_num}-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": f"INF by Store - Part {batch_num}/{len(batches)}",
                        "subtitle": f"Showing {len(batch)} stores",
                        "imageUrl": "https://cdn-icons-png.flaticon.com/512/869/869636.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": sections_stores,
                },
            }]
        }
        
        # Send this batch
        try:
            # Create fresh connector for each batch
            batch_ssl_context = ssl.create_default_context(cafile=certifi.where())
            batch_connector = aiohttp.TCPConnector(ssl=batch_ssl_context)
            
            async with aiohttp.ClientSession(timeout=timeout, connector=batch_connector) as session:
                async with session.post(CHAT_WEBHOOK_URL, json=payload_stores) as resp:
                    if resp.status != 200:
                        app_logger.error(f"Failed to send store batch {batch_num}: {await resp.text()}")
                    else:
                        app_logger.info(f"Store batch {batch_num}/{len(batches)} sent successfully.")
            
            # Small delay between messages to avoid rate limiting
            if batch_num < len(batches):
                await asyncio.sleep(0.5)
                
        except Exception as e:
            app_logger.error(f"Error sending store batch {batch_num}: {e}")

async def main():
    app_logger.info("Starting INF Scraper with Dynamic Concurrency...")
    
    # Load stores
    urls_data = []
    load_default_data(urls_data, app_logger)
    if not urls_data:
        app_logger.error("No stores found.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not DEBUG_MODE)
        
        # Auth
        if not ensure_storage_state(STORAGE_STATE, app_logger):
             app_logger.info("State missing, attempting login...")
             page = await browser.new_page()
             if not await perform_login_and_otp(page, LOGIN_URL, config, PAGE_TIMEOUT, DEBUG_MODE, app_logger, _save_screenshot):
                 app_logger.error("Login failed.")
                 return
             await page.context.storage_state(path=STORAGE_STATE)
             await page.close()

        # Load state
        with open(STORAGE_STATE) as f:
            storage_state = json.load(f)
            
        # Setup Queue
        job_queue = Queue()
        for store in urls_data:
            job_queue.put_nowait(store)
            
        results_list = []
        results_lock = Lock()
        
        # Concurrency State
        concurrency_limit_ref = {'value': INITIAL_CONCURRENCY}
        active_workers_ref = {'value': 0}
        concurrency_condition = Condition()
        last_concurrency_change_ref = {'value': 0.0}
        
        failure_lock = Lock()
        failure_timestamps = []
        
        # Start Auto-concurrency Manager
        if AUTO_ENABLED:
            asyncio.create_task(auto_concurrency_manager(
                concurrency_limit_ref, last_concurrency_change_ref, AUTO_ENABLED, AUTO_MIN_CONCURRENCY,
                AUTO_MAX_CONCURRENCY, CPU_UPPER_THRESHOLD, CPU_LOWER_THRESHOLD, MEM_UPPER_THRESHOLD,
                CHECK_INTERVAL, COOLDOWN_SECONDS, failure_lock, failure_timestamps,
                concurrency_condition, app_logger
            ))
        
        # Launch Workers
        # We launch enough workers to cover the MAX concurrency, but they will be throttled by the condition
        num_workers = min(AUTO_MAX_CONCURRENCY, len(urls_data))
        app_logger.info(f"Launching {num_workers} workers (Initial Concurrency Limit: {INITIAL_CONCURRENCY})...")
        
        workers = [
            asyncio.create_task(worker(i+1, browser, storage_state, job_queue, results_list, results_lock,
                                       concurrency_limit_ref, active_workers_ref, concurrency_condition,
                                       failure_lock, failure_timestamps))
            for i in range(num_workers)
        ]
        
        await asyncio.gather(*workers)
        
        # Process Results
        # results_list contains tuples of (store_name, store_number, items)
        all_items = []
        
        for store_name, store_number, items in results_list:
            all_items.extend(items)
        
        # Calculate Network Wide Top 10
        aggregated = {}
        for item in all_items:
            key = (item['sku'], item['name'])
            if key not in aggregated:
                aggregated[key] = 0
            aggregated[key] += item['inf']
            
        network_list = [{"sku": k[0], "name": k[1], "inf": v, "store": "All Stores"} for k, v in aggregated.items()]
        network_list.sort(key=lambda x: x['inf'], reverse=True)
        network_top_10 = network_list[:10]
        
        # Send Report - pass the full results_list with store numbers
        await send_inf_report(results_list, network_top_10)
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
