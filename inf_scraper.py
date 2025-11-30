
import logging
import json
import asyncio
import re
import os
import csv
import base64
import io
from datetime import datetime
from typing import List, Dict
from asyncio import Queue, Lock, Condition
from playwright.async_api import async_playwright, Page, TimeoutError, expect, Browser
import qrcode
from pytz import timezone
import urllib.parse

# Import modules
from utils import setup_logging, sanitize_store_name, _save_screenshot, load_default_data, ensure_storage_state, LOCAL_TIMEZONE
from auth import check_if_login_needed, perform_login_and_otp, prime_master_session
from workers import auto_concurrency_manager
from stock_enrichment import enrich_items_with_stock_data
from date_range import get_date_time_range_from_config, apply_date_time_range

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
CHAT_WEBHOOK_URL = config.get('inf_webhook_url') or config.get('chat_webhook_url')
APPS_SCRIPT_URL = config.get('apps_script_webhook_url')  # Optional - for interactive buttons
STORAGE_STATE = 'state.json'
OUTPUT_DIR = 'output'
PAGE_TIMEOUT = config.get('page_timeout_ms', 30000)
STORE_PREFIX_RE = re.compile(r"^morrisons\s*-\s*", re.I)

# Morrisons API Config
MORRISONS_API_KEY = config.get('morrisons_api_key')
MORRISONS_BEARER_TOKEN_URL = config.get('morrisons_bearer_token_url') or "https://gist.githubusercontent.com/Daave2/b62faeed0dd435100773d4de775ff52d/raw/gistfile1.txt"
ENRICH_STOCK_DATA = config.get('enrich_stock_data', True)  # Enabled by default

# Fetch bearer token from gist at startup
MORRISONS_BEARER_TOKEN = None
if ENRICH_STOCK_DATA and MORRISONS_BEARER_TOKEN_URL:
    from stock_enrichment import fetch_bearer_token_from_gist
    app_logger.info(f"Fetching bearer token from: {MORRISONS_BEARER_TOKEN_URL}")
    MORRISONS_BEARER_TOKEN = fetch_bearer_token_from_gist(MORRISONS_BEARER_TOKEN_URL)
    if MORRISONS_BEARER_TOKEN:
        app_logger.info("Bearer token successfully loaded")
    else:
        app_logger.warning("Failed to fetch bearer token - stock enrichment will fail!")


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
        except (TimeoutError, AssertionError):
            app_logger.info(f"[{store_name}] No data rows found (or table not visible); returning empty list.")
            # Take a debug screenshot to verify if it's truly empty or a loading issue
            await _save_screenshot(page, f"debug_empty_{sanitize_store_name(store_name, STORE_PREFIX_RE)}", "output", timezone('Europe/London'), app_logger)
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
                img_url = await cells.nth(0).locator("img").get_attribute("src")
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
                    "inf": inf_value,
                    "image_url": img_url
                })
            except Exception as e:
                app_logger.warning(f"[{store_name}] Error extracting row {i}: {e}")
        
        app_logger.info(f"[{store_name}] Extracted {len(extracted_data)} items.")
        return extracted_data

    except Exception as e:
        app_logger.error(f"[{store_name}] Error processing INF page: {e}")
        await _save_screenshot(page, f"error_inf_{sanitize_store_name(store_name, STORE_PREFIX_RE)}", OUTPUT_DIR, LOCAL_TIMEZONE, app_logger)
        return []

async def process_store_task(context, store_info, results_list, results_lock, failure_lock, failure_timestamps, date_range_func=None, action_timeout=20000):
    merchant_id = store_info['merchant_id']
    marketplace_id = store_info['marketplace_id']
    store_name = store_info['store_name']
    store_number = store_info.get('store_number', '')
    inf_rate = store_info.get('inf_rate', 'N/A')
    
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
        
        # Apply date range if configured (same as main scraper)
        if date_range_func:
            date_range_applied = await apply_date_time_range(
                page, store_name, date_range_func, action_timeout, DEBUG_MODE, app_logger
            )
            if date_range_applied:
                app_logger.info(f"[{store_name}] Date range applied to INF page")
            else:
                app_logger.warning(f"[{store_name}] Could not apply date range to INF page, using default")
        
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
            results_list.append((store_name, store_number, items, inf_rate))
            
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
                 failure_lock: Lock, failure_timestamps: List, date_range_func=None, action_timeout=20000):
    
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
                await process_store_task(context, store_info, results_list, results_lock, failure_lock, failure_timestamps, date_range_func, action_timeout)
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

def generate_qr_code_data_url(sku: str) -> str:
    """Generate a QR code as a data URL for embedding in Google Chat."""
    try:
        # Generate QR code
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=2,
        )
        qr.add_data(sku)
        qr.make(fit=True)
        
        # Create an image
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Convert to base64 data URL
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        img_str = base64.b64encode(buffer.getvalue()).decode()
        return f"data:image/png;base64,{img_str}"
    except Exception as e:
        app_logger.warning(f"Failed to generate QR code for SKU {sku}: {e}")
        return ""


async def send_inf_report(store_data, network_top_10, skip_network_report=False, title_prefix="", top_n=5):
    """Send INF report to Google Chat
    
    Args:
        store_data: List of tuples (store_name, store_number, items, inf_rate)
        network_top_10: List of top 10 items network-wide
        skip_network_report: If True, skip sending the network-wide summary
        title_prefix: Optional prefix for the report title (e.g. "Yesterday's ")
        top_n: Number of top items to show per store (5, 10, 25)
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
    
    # Message 1: Network Wide Top 10 (skip if requested)
    if not skip_network_report:
        sections_network = []
        widgets_network = []
        widgets_network.append({"textParagraph": {"text": "<b>🏆 Top 10 Network Wide (INF Occurrences)</b>"}})
        
        for item in network_top_10:
            text = f"<b>{item['inf']}</b> - {item['name']}"
            widgets_network.append({"textParagraph": {"text": text}})
            
        sections_network.append({"widgets": widgets_network})
        
        # Add Quick Actions if Apps Script URL is available
        if APPS_SCRIPT_URL:
            # Helper to build URL
            def build_trigger_url(event_type, date_mode, top_n_val):
                params = {'event_type': event_type, 'date_mode': date_mode, 'top_n': top_n_val}
                return f"{APPS_SCRIPT_URL}?{urllib.parse.urlencode(params)}"

            sections_network.append({
                "header": "⚡ Quick Actions",
                "widgets": [
                    {
                        "buttonList": {
                            "buttons": [
                                {
                                    "text": "🔄 Re-run Analysis (Today)",
                                    "onClick": {
                                        "openLink": {
                                            "url": build_trigger_url("run-inf-analysis", "today", str(top_n))
                                        }
                                    }
                                },
                                {
                                    "text": "📅 Yesterday's Report",
                                    "onClick": {
                                        "openLink": {
                                            "url": build_trigger_url("run-inf-analysis", "yesterday", str(top_n))
                                        }
                                    }
                                }
                            ]
                        }
                    }
                ]
            })

        payload_network = {
            "cardsV2": [{
                "cardId": f"inf-network-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": f"{title_prefix}INF Analysis - Network Wide",
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
    
    # Message 2+: All Stores (sorted alphabetically)
    sorted_store_data = sorted(store_data, key=lambda x: x[0])
    stores_with_data = [(name, num, items, inf_rate) for name, num, items, inf_rate in sorted_store_data if items]
    
    # Dynamic batch size based on items shown
    # More items per store = fewer stores per batch to avoid payload limits
    if top_n <= 5:
        BATCH_SIZE = 8  # Reduced from 10 for safety
    elif top_n <= 10:
        BATCH_SIZE = 4  # Reduced from 7 due to payload errors
    else:  # top_n >= 25
        BATCH_SIZE = 3
    
    batches = [stores_with_data[i:i + BATCH_SIZE] for i in range(0, len(stores_with_data), BATCH_SIZE)]
    
    for batch_num, batch in enumerate(batches, 1):
        sections_stores = []
        
        for store_name, store_number, items, inf_rate in batch:
            widgets_store = []
            clean_store_name = sanitize_store_name(store_name, STORE_PREFIX_RE)
            total_inf = sum(item['inf'] for item in items)
            
            # Header with INF Rate
            inf_display = f"INF: {inf_rate}" if inf_rate != 'N/A' else f"Total INF: {total_inf}"
            section_header = f"{clean_store_name} | {inf_display}"
            
            # Show top N items with card layout
            for item in items[:top_n]:
                # Build Columns Widget
                img_url = item.get('image_url', '')
                sku = item['sku']
                
                # Text Details (Left Column)
                details = f"<b>{item['name']}</b>\n\n"
                details += f"🔴 <b>INF Count: {item['inf']}</b>\n"
                details += f"📦 SKU: <font color='#1a73e8'>{sku}</font>\n"
                
                # Add barcode if available
                if item.get('barcode'):
                    details += f"🔢 EAN: {item['barcode']}\n"
                
                # Add price if available
                if item.get('price') is not None:
                    details += f"💷 £{item['price']:.2f}\n"
                
                # Add discontinuation warning if product is not active
                # Only show if:
                # 1. We have valid status data from API (not None)
                # 2. Item has no location (if it has a location, it's still ranged)
                # 3. Status indicates discontinued
                has_location = item.get('std_location') or item.get('promo_location')
                has_status_data = item.get('product_status') is not None or item.get('commercially_active') is not None
                is_discontinued = item.get('product_status') != 'A' and item.get('commercially_active') != 'Yes'
                
                if has_status_data and not has_location and is_discontinued:
                    details += f"\n🚫 <b><font color='#d93025'>DISCONTINUED/NOT RANGED</font></b>"
                
                if ENRICH_STOCK_DATA:
                    stock_qty = item.get('stock_on_hand')
                    if stock_qty is not None:
                        stock_unit = item.get('stock_unit', '')
                        details += f"\n📊 Stock: {stock_qty} {stock_unit}"
                        
                        if item.get('stock_last_updated'):
                            # Format timestamp if needed, or just display
                            # Assuming ISO format or similar readable string from API
                            ts = item['stock_last_updated']
                            # Try to make it more readable if it's a long ISO string
                            try:
                                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                                ts_display = dt.strftime("%H:%M")
                                details += f" (at {ts_display})"
                            except:
                                pass # Keep silent if parsing fails
                    else:
                        details += f"\n📊 Stock: Not Found"
                
                if item.get('std_location'):
                    details += f"\n📍 {item['std_location']}"
                
                if item.get('promo_location'):
                    details += f"\n🏷️ {item['promo_location']}"
                
                # Column 1: Text Details (LEFT)
                col1_widgets = [{
                    "textParagraph": {
                        "text": details
                    }
                }]
                
                # Prepare QR code URL
                import urllib.parse
                encoded_sku = urllib.parse.quote(sku)
                qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=75x75&data={encoded_sku}"
                
                # Prepare high-res product image URL
                high_res_url = img_url
                if img_url:
                    # Skip resizing for Morrisons/Brandbank images
                    if 'brandbank' in img_url.lower():
                        high_res_url = img_url
                    else:
                        # Use regex to replace any Amazon image size parameters with high-res version
                        high_res_url = re.sub(r'_S[LXY]\d+_', '_SL500_', img_url)
                        high_res_url = re.sub(r'_AC_UL\d+_', '_AC_UL500_', high_res_url)
                        # If no size parameters found, try adding one
                        if high_res_url == img_url and ('.jpg' in high_res_url or '.png' in high_res_url):
                            high_res_url = re.sub(r'\.(jpg|png)', r'._SL500_.\1', high_res_url)
                
                # Build right column widgets: text details + product image
                right_column_widgets = [
                    {
                        "textParagraph": {
                            "text": details
                        }
                    }
                ]
                
                # Add product image if available
                if img_url:
                    right_column_widgets.append({
                        "image": {
                            "imageUrl": high_res_url,
                            "altText": f"Product image for {sku}"
                        }
                    })
                
                # Build columns layout: QR (left, compact) | Details + Image (right, fill)
                columns_widget = {
                    "columns": {
                        "columnItems": [
                            {
                                "horizontalSizeStyle": "FILL_MINIMUM_SPACE",
                                "horizontalAlignment": "CENTER",
                                "verticalAlignment": "CENTER",
                                "widgets": [{"image": {"imageUrl": qr_url, "altText": f"QR code for SKU {sku}"}}]
                            },
                            {
                                "horizontalSizeStyle": "FILL_AVAILABLE_SPACE",
                                "widgets": right_column_widgets
                            }
                        ]
                    }
                }
                
                widgets_store.append(columns_widget)
                widgets_store.append({"divider": {}})

            
            # Add collapsible section
            sections_stores.append({
                "header": section_header,
                "collapsible": True,
                "uncollapsibleWidgetsCount": 0,
                "widgets": widgets_store
            })
        
        payload_stores = {
            "cardsV2": [{
                "cardId": f"inf-stores-{batch_num}-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": f"{title_prefix}INF by Store - {datetime.now(LOCAL_TIMEZONE).strftime('%H:%M')} - Part {batch_num}/{len(batches)}",
                        "subtitle": f"Showing {len(batch)} stores",
                        "imageUrl": "https://cdn-icons-png.flaticon.com/512/869/869636.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": sections_stores,
                },
            }]
        }
        
        # Send this batch with retry logic for rate limits
        max_retries = 3
        retry_delay = 2.0  # Start with 2 seconds
        
        for attempt in range(max_retries):
            try:
                # Create fresh connector for each batch
                batch_ssl_context = ssl.create_default_context(cafile=certifi.where())
                batch_connector = aiohttp.TCPConnector(ssl=batch_ssl_context)
                
                async with aiohttp.ClientSession(timeout=timeout, connector=batch_connector) as session:
                    async with session.post(CHAT_WEBHOOK_URL, json=payload_stores) as resp:
                        if resp.status == 429:
                            # Rate limit hit - retry with exponential backoff
                            if attempt < max_retries - 1:
                                wait_time = retry_delay * (2 ** attempt)
                                app_logger.warning(f"Rate limit hit for batch {batch_num}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                app_logger.error(f"Failed to send store batch {batch_num} after {max_retries} attempts: {await resp.text()}")
                                break
                        elif resp.status != 200:
                            app_logger.error(f"Failed to send store batch {batch_num}: {await resp.text()}")
                            break
                        else:
                            app_logger.info(f"Store batch {batch_num}/{len(batches)} sent successfully.")
                            break
                
                # Delay between batches to avoid rate limiting (1.5s is safer than 0.5s)
                if batch_num < len(batches):
                    await asyncio.sleep(1.5)
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    app_logger.warning(f"Error sending store batch {batch_num} (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    app_logger.error(f"Error sending store batch {batch_num} after {max_retries} attempts: {e}")


async def run_inf_analysis(target_stores: List[Dict] = None, provided_browser: Browser = None, config_override: Dict = None):
    app_logger.info("Starting INF Analysis...")
    
    # Load stores if not provided
    if target_stores is None:
        urls_data = []
        load_default_data(urls_data, app_logger)
        if not urls_data:
            app_logger.error("No stores found.")
            return
    else:
        urls_data = target_stores
        app_logger.info(f"Analyzing {len(urls_data)} provided stores.")

    # Manage browser lifecycle
    local_playwright = None
    browser = provided_browser
    
    try:
        if not browser:
            local_playwright = await async_playwright().start()
            browser = await local_playwright.chromium.launch(headless=not DEBUG_MODE)
        
        # Auth - only check/login if we're managing our own browser
        # If browser was provided by main scraper, it's already authenticated
        if not provided_browser:
            login_needed = True
            if ensure_storage_state(STORAGE_STATE, app_logger):
                app_logger.info("State file found, verifying session...")
                try:
                    # Create a temporary context to check login status
                    temp_context = await browser.new_context(storage_state=STORAGE_STATE)
                    temp_page = await temp_context.new_page()
                    
                    # Check if we are actually logged in
                    test_url = "https://sellercentral.amazon.co.uk/home"
                    if not await check_if_login_needed(temp_page, test_url, PAGE_TIMEOUT, DEBUG_MODE, app_logger):
                        app_logger.info("Session is valid.")
                        login_needed = False
                    else:
                        app_logger.info("Session is invalid or expired.")
                    
                    await temp_context.close()
                except Exception as e:
                    app_logger.error(f"Error verifying session: {e}")
            
            if login_needed:
                 app_logger.info("Performing login...")
                 page = await browser.new_page()
                 if not await perform_login_and_otp(page, LOGIN_URL, config, PAGE_TIMEOUT, DEBUG_MODE, app_logger, _save_screenshot):
                     app_logger.error("Login failed.")
                     if local_playwright: await local_playwright.stop()
                     return
                 await page.context.storage_state(path=STORAGE_STATE)
                 await page.close()
        else:
            app_logger.info("Using provided browser from main scraper (already authenticated)")

        # Load state
        with open(STORAGE_STATE) as f:
            storage_state = json.load(f)
        
        # Use overridden config if provided, otherwise use global config
        active_config = config_override if config_override else config

        # Create date range function (same as main scraper)
        def get_date_range():
            return get_date_time_range_from_config(active_config, LOCAL_TIMEZONE, app_logger)
        
        # Determine ACTION_TIMEOUT
        ACTION_TIMEOUT = int(PAGE_TIMEOUT / 2)
            
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
        num_workers = min(AUTO_MAX_CONCURRENCY, len(urls_data))
        app_logger.info(f"Launching {num_workers} workers (Initial Concurrency Limit: {INITIAL_CONCURRENCY})...")
        
        workers = [
            asyncio.create_task(worker(i+1, browser, storage_state, job_queue, results_list, results_lock,
                                       concurrency_limit_ref, active_workers_ref, concurrency_condition,
                                       failure_lock, failure_timestamps, get_date_range, ACTION_TIMEOUT))
            for i in range(num_workers)
        ]
        
        await asyncio.gather(*workers)
        
        # Process Results
        # results_list contains tuples of (store_name, store_number, items, inf_rate)
        all_items = []
        
        for store_name, store_number, items, inf_rate in results_list:
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
        
        # Determine title prefix based on date mode
        title_prefix = ""
        if active_config.get('use_date_range'):
            mode = active_config.get('date_range_mode')
            if mode == 'today':
                title_prefix = "Today's "
            elif mode == 'yesterday':
                title_prefix = "Yesterday's "
            elif mode == 'last_7_days':
                title_prefix = "Last 7 Days "
            elif mode == 'last_30_days':
                title_prefix = "Last 30 Days "
            elif mode == 'week_to_date':
                title_prefix = "Week to Date "
            elif mode == 'custom':
                # Check if it's actually "Today" (custom dates matching today)
                try:
                    today_str = datetime.now(LOCAL_TIMEZONE).strftime("%m/%d/%Y")
                    if active_config.get('custom_start_date') == today_str and active_config.get('custom_end_date') == today_str:
                        title_prefix = "Today's "
                    else:
                        title_prefix = "Custom Range "
                except:
                    title_prefix = "Custom Range "

        # Send Report - skip network-wide report if called from main scraper with specific stores
        skip_network = target_stores is not None
        top_n = active_config.get('top_n_items', 5)  # Default to 5 if not specified
        await send_inf_report(results_list, network_top_10, skip_network_report=skip_network, title_prefix=title_prefix, top_n=top_n)
        
        # Send Quick Actions card if not skipped (i.e., standalone run)
        if not skip_network and APPS_SCRIPT_URL:
            from webhook import post_quick_actions_card
            await post_quick_actions_card(CHAT_WEBHOOK_URL, APPS_SCRIPT_URL, DEBUG_MODE, app_logger)
        
    finally:
        if local_playwright:
            if browser: await browser.close()
            await local_playwright.stop()

async def main():
    import argparse
    
    # CLI Argument Parsing
    parser = argparse.ArgumentParser(description='INF Scraper (Standalone)')
    parser.add_argument('--date-mode', choices=['today', 'yesterday', 'last_7_days', 'last_30_days', 'relative', 'custom'], help='Date range mode')
    parser.add_argument('--start-date', help='Start date (MM/DD/YYYY)')
    parser.add_argument('--end-date', help='End date (MM/DD/YYYY)')
    parser.add_argument('--start-time', help='Start time (e.g., "12:00 AM")')
    parser.add_argument('--end-time', help='End time (e.g., "11:59 PM")')
    parser.add_argument('--relative-days', type=int, help='Days offset for relative mode')
    
    args, unknown = parser.parse_known_args()
    
    # Create a copy of the global config to modify
    local_config = config.copy()
    
    # Merge CLI args into config
    if args.date_mode:
        local_config['use_date_range'] = True
        local_config['date_range_mode'] = args.date_mode

    if args.start_date: local_config['custom_start_date'] = args.start_date
    if args.end_date: local_config['custom_end_date'] = args.end_date
    if args.start_time: local_config['custom_start_time'] = args.start_time
    if args.end_time: local_config['custom_end_time'] = args.end_time
    if args.relative_days is not None: local_config['relative_days'] = args.relative_days
    
    # Force custom mode if dates provided without mode
    if (args.start_date or args.end_date) and not args.date_mode:
        local_config['use_date_range'] = True
        local_config['date_range_mode'] = 'custom'

    await run_inf_analysis(config_override=local_config)

if __name__ == "__main__":
    asyncio.run(main())
