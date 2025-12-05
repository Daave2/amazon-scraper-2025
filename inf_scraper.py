
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
from urllib.parse import urlencode

# Import modules
from utils import (
    setup_logging,
    sanitize_store_name,
    sanitize_csv_value,
    _save_screenshot,
    load_default_data,
    ensure_storage_state,
    LOCAL_TIMEZONE,
)
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

def upload_csv_to_gist(csv_file_path: str, description: str) -> str:
    """
    Upload a CSV file to GitHub Gist and return the raw file URL.
    Only works when running in GitHub Actions (GIST_TOKEN available).
    
    Args:
        csv_file_path: Path to the CSV file
        description: Description for the Gist
        
    Returns:
        Raw file URL of the uploaded Gist, or empty string on failure
    """
    import requests
    
    # Check if running in GitHub Actions
    github_token = os.environ.get('GIST_TOKEN')
    if not github_token:
        app_logger.debug("GIST_TOKEN not found - skipping Gist upload")
        return ""
    
    try:
        # Read CSV file content
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        # Get filename from path
        filename = os.path.basename(csv_file_path)
        
        # Prepare Gist payload
        gist_data = {
            "description": description,
            "public": True,
            "files": {
                filename: {
                    "content": csv_content
                }
            }
        }
        
        # Upload to GitHub
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        response = requests.post(
            "https://api.github.com/gists",
            headers=headers,
            json=gist_data,
            timeout=30
        )
        
        if response.status_code == 201:
            gist_data = response.json()
            # Get the raw URL for direct CSV download
            raw_url = gist_data['files'][filename]['raw_url']
            app_logger.info(f"Successfully uploaded {filename} to Gist: {raw_url}")
            return raw_url
        else:
            app_logger.error(f"Failed to upload Gist: {response.status_code} - {response.text}")
            return ""
            
    except Exception as e:
        app_logger.error(f"Error uploading CSV to Gist: {e}")
        return ""


async def navigate_and_extract_inf(page: Page, store_name: str, top_n: int = 10):
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

        # Extract Data - top N rows based on configuration
        rows = await page.locator(f"{table_sel} tr").all()
        app_logger.info(f"[{store_name}] Found {len(rows)} rows; extracting top {top_n}")
        
        extracted_data = []
        
        for i, row in enumerate(rows[:top_n]):
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

async def process_store_task(context, store_info, results_list, results_lock, failure_lock, failure_timestamps, date_range_func=None, action_timeout=20000, bearer_token=None, top_n=10):
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
        items = await navigate_and_extract_inf(page, store_name, top_n)
        
        # Enrich with stock data if enabled and we have a store number
        if ENRICH_STOCK_DATA and store_number and items and MORRISONS_API_KEY:
            try:
                app_logger.info(f"[{store_name}] Enriching {len(items)} items with stock data...")
                token_status = "valid token" if bearer_token else "NO TOKEN"
                token_preview = f"{bearer_token[:20]}..." if bearer_token and len(bearer_token) > 20 else "None"
                app_logger.debug(f"[{store_name}] Bearer token status: {token_status} (preview: {token_preview})")
                items = await enrich_items_with_stock_data(
                    items, 
                    store_number, 
                    MORRISONS_API_KEY, 
                    bearer_token  # Use the fresh token passed as parameter
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
                 failure_lock: Lock, failure_timestamps: List, date_range_func=None, action_timeout=20000, bearer_token=None, top_n=10):
    
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
                await process_store_task(context, store_info, results_list, results_lock, failure_lock, failure_timestamps, date_range_func, action_timeout, bearer_token, top_n)
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


async def send_inf_report(store_data, network_top_10, skip_network_report=False, title_prefix="", top_n=5, csv_urls=None):
    """Send INF report to Google Chat
    
    Args:
        store_data: List of tuples (store_name, store_number, items, inf_rate)
        network_top_10: List of top 10 items network-wide
        skip_network_report: If True, skip sending the network-wide summary
        title_prefix: Optional prefix for the report title (e.g. "Yesterday's ")
        top_n: Number of top items to show per store (5, 10, 25)
        csv_urls: Optional dict with CSV download URLs (keys: 'store_details', 'network_summary')
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
        widgets_network.append({"textParagraph": {"text": "<b>⚠️ Top 10 Network Wide (INF Occurrences)</b>"}})
        
        for item in network_top_10:
            # Build clean store name without prefix
            top_stores_formatted = []
            for store_name, inf_count, store_number in item['top_stores']:
                clean_name = sanitize_store_name(store_name, STORE_PREFIX_RE)
                top_stores_formatted.append(f"{clean_name} {inf_count}")
            
            stores_text = ", ".join(top_stores_formatted)
            store_summary = f"({item['store_count']} stores: {stores_text})"
            
            # Build text with INF count, product name, and store breakdown
            text = f"<b>{item['inf']}</b> - {item['name']}<br>"
            text += f"<font color='#666666'>{store_summary}</font>"
            
            # Add price and SKU if available
            details = []
            if item.get('price') is not None:
                details.append(f"£{item['price']:.2f}")
            if item.get('sku'):
                details.append(f"SKU: {item['sku']}")
            
            if details:
                text += f"<br><font color='#888888'>{' | '.join(details)}</font>"
            
            widgets_network.append({"textParagraph": {"text": text}})
        
        # Build Network Analysis URL for all top 10 items
        # Format: /network/SKU:StoreId=InfCount:StoreId=InfCount,NextSKU:StoreId=InfCount...
        inventory_url = config.get('inventory_system_url', '')
        if inventory_url and network_top_10:
            # Extract base URL (e.g., https://app.218.team from https://app.218.team/assistant/{sku}...)
            base_url = inventory_url.split('/assistant/')[0] if '/assistant/' in inventory_url else ''
            
            if base_url:
                network_payload_parts = []
                for item in network_top_10:
                    sku = item['sku']
                    store_parts = []
                    for store_name, inf_count, store_number in item['top_stores']:
                        if store_number:  # Only include stores with valid store numbers
                            store_parts.append(f"{store_number}={inf_count}")
                    
                    if store_parts:
                        # Format: SKU:StoreId=InfCount:StoreId=InfCount
                        sku_payload = f"{sku}:" + ":".join(store_parts)
                        network_payload_parts.append(sku_payload)
                
                if network_payload_parts:
                    network_url = f"{base_url}/#/network/{','.join(network_payload_parts)}"
                    widgets_network.append({
                        "buttonList": {
                            "buttons": [{
                                "text": "🌐 View Network Analysis",
                                "onClick": {
                                    "openLink": {
                                        "url": network_url
                                    }
                                }
                            }]
                        }
                    })
            
        sections_network.append({"widgets": widgets_network})
        
        # Add CSV Downloads section if URLs are available
        if csv_urls and (csv_urls.get('store_details') or csv_urls.get('network_summary')):
            csv_buttons = []
            
            if csv_urls.get('store_details'):
                csv_buttons.append({
                    "text": "📥 Download Store Details CSV",
                    "onClick": {
                        "openLink": {
                            "url": csv_urls['store_details']
                        }
                    }
                })
            
            if csv_urls.get('network_summary'):
                csv_buttons.append({
                    "text": "📥 Download Network Summary CSV",
                    "onClick": {
                        "openLink": {
                            "url": csv_urls['network_summary']
                        }
                    }
                })
            
            if csv_buttons:
                sections_network.append({
                    "header": "📊 Download CSV Data",
                    "widgets": [
                        {
                            "textParagraph": {
                                "text": (
                                    "<i>Tip: To download the CSV, open the link, then "
                                    "use your browser's Save As (Ctrl+S / Cmd+S) and "
                                    "keep the .csv extension.</i>"
                                )
                            }
                        },
                        {
                            "buttonList": {
                                "buttons": csv_buttons
                            }
                        }
                    ]
                })
        
        # Add Quick Actions if Apps Script URL is available
        if APPS_SCRIPT_URL:
            # Helper to build URL
            def build_trigger_url(event_type, date_mode, top_n_val):
                params = {'event_type': event_type, 'date_mode': date_mode, 'top_n': top_n_val}
                return f"{APPS_SCRIPT_URL}?{urlencode(params)}"

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
    # With simplified text-only format (no images/QR codes), we can fit MANY more stores per batch
    if top_n <= 5:
        BATCH_SIZE = 30  # Was 8 - much smaller payloads now with text-only format
    elif top_n <= 10:
        BATCH_SIZE = 20  # Was 4 - simplified cards allow more stores
    else:  # top_n >= 25
        BATCH_SIZE = 15  # Was 3 - even with 25 items, text is tiny compared to images
    
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
            
            # Build product list text (no images/QR codes)
            product_lines = []
            for item in items[:top_n]:
                line = f"• <b>{item['name']}</b>"
                line += f" - <b>{item['inf']}</b> INF"
                line += f" (SKU: {item['sku']})"
                
                # Add price if available
                if item.get('price') is not None:
                    line += f" - £{item['price']:.2f}"
                
                product_lines.append(line)
            
            # Add product list as single text paragraph
            if product_lines:
                widgets_store.append({
                    "textParagraph": {
                        "text": "\n".join(product_lines)
                    }
                })
            
            # Build aggregated link to external app using inventory_system_url from config
            # Format: https://app.218.team/#/amazon/SKU1:INF1,SKU2:INF2?locationId=066
            inventory_url = config.get('inventory_system_url', '')
            if store_number and items and inventory_url:
                # Extract base URL (e.g., https://app.218.team from https://app.218.team/assistant/{sku}...)
                base_url = inventory_url.split('/assistant/')[0] if '/assistant/' in inventory_url else ''
                
                if base_url:
                    # Build product string: SKU1:INF1,SKU2:INF2,...
                    product_params = ",".join([f"{item['sku']}:{item['inf']}" for item in items[:top_n]])
                    analysis_url = f"{base_url}/#/amazon/{product_params}?locationId={store_number}"
                    
                    # Add buttons: View Products and Auto PDF
                    widgets_store.append({
                        "buttonList": {
                            "buttons": [
                                {
                                    "text": f"📊 View All {len(items[:top_n])} Products",
                                    "onClick": {
                                        "openLink": {
                                            "url": analysis_url
                                        }
                                    }
                                },
                                {
                                    "text": "📄 Auto PDF",
                                    "onClick": {
                                        "openLink": {
                                            "url": f"{analysis_url}&pdf"
                                        }
                                    }
                                }
                            ]
                        }
                    })
            
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

        # Fetch fresh bearer token for this run (tokens expire frequently)
        bearer_token_for_run = MORRISONS_BEARER_TOKEN  # Start with global token
        if ENRICH_STOCK_DATA and MORRISONS_BEARER_TOKEN_URL:
            app_logger.info("Fetching fresh bearer token for this INF run...")
            from stock_enrichment import fetch_bearer_token_from_gist
            fresh_token = fetch_bearer_token_from_gist(MORRISONS_BEARER_TOKEN_URL)
            if fresh_token:
                bearer_token_for_run = fresh_token
                token_preview = f"{fresh_token[:20]}..." if len(fresh_token) > 20 else fresh_token
                app_logger.info(f"Fresh bearer token successfully loaded (preview: {token_preview})")
            else:
                app_logger.warning("Failed to fetch fresh bearer token - will use global token (may be expired)")
        
        # Log final token status for debugging
        if bearer_token_for_run:
            app_logger.info(f"Bearer token is set and ready (length: {len(bearer_token_for_run)})")
        else:
            app_logger.warning("WARNING: Bearer token is None! Stock enrichment will fail with 401 errors")


        # Load state
        with open(STORAGE_STATE) as f:
            storage_state = json.load(f)
        
        # Use overridden config if provided, otherwise use global config
        active_config = config_override if config_override else config
        apps_script_url = active_config.get('apps_script_webhook_url') or APPS_SCRIPT_URL
        skip_network = target_stores is not None
        should_post_quick_actions = (not skip_network) and bool(CHAT_WEBHOOK_URL) and bool(apps_script_url)

        # Create date range function (same as main scraper)
        def get_date_range():
            return get_date_time_range_from_config(active_config, LOCAL_TIMEZONE, app_logger)
        
        # Determine ACTION_TIMEOUT
        ACTION_TIMEOUT = int(PAGE_TIMEOUT / 2)
        
        # Get top_n for extraction (default 10)
        top_n = active_config.get('top_n_items', 10)
            
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
                                       failure_lock, failure_timestamps, get_date_range, ACTION_TIMEOUT, bearer_token_for_run, top_n))
            for i in range(num_workers)
        ]
        
        await asyncio.gather(*workers)
        
        # Process Results
        # results_list contains tuples of (store_name, store_number, items, inf_rate)
        all_items = []
        
        # Build a mapping of store_name -> store_number for network analysis URLs
        store_number_map = {}
        for store_name, store_number, items, inf_rate in results_list:
            store_number_map[store_name] = store_number
            # Add store_number to each item for tracking
            for item in items:
                item['store_number'] = store_number
            all_items.extend(items)
        
        # Calculate Network Wide Top 25 with store breakdown
        aggregated = {}
        for item in all_items:
            key = (item['sku'], item['name'])
            if key not in aggregated:
                aggregated[key] = {
                    'total_inf': 0,
                    'stores': {},  # store_name -> {'inf': count, 'store_number': number}
                    'image_url': item.get('image_url', ''),
                    'barcode': item.get('barcode'),
                    'price': item.get('price')
                }
            aggregated[key]['total_inf'] += item['inf']
            
            # Track store contribution with store number
            store_name = item['store']
            store_number = item.get('store_number', '')
            if store_name not in aggregated[key]['stores']:
                aggregated[key]['stores'][store_name] = {'inf': 0, 'store_number': store_number}
            aggregated[key]['stores'][store_name]['inf'] += item['inf']
            
        # Build network list with top contributing stores (up to 10) and all stores for CSV
        network_list = []
        for (sku, name), data in aggregated.items():
            # Sort stores by INF contribution - now stores is dict with 'inf' and 'store_number'
            sorted_stores = sorted(data['stores'].items(), key=lambda x: x[1]['inf'], reverse=True)
            # Convert to list of tuples: (store_name, inf_count, store_number)
            top_stores = [(name, info['inf'], info['store_number']) for name, info in sorted_stores[:10]]
            all_stores = [(name, info['inf'], info['store_number']) for name, info in sorted_stores]

            network_list.append({
                "sku": sku,
                "name": name,
                "inf": data['total_inf'],
                "top_stores": top_stores,  # [(store_name, inf_count, store_number), ...]
                "all_stores": all_stores,
                "store_count": len(data['stores']),
                "image_url": data['image_url'],
                "barcode": data['barcode'],
                "price": data['price']
            })

        network_list.sort(key=lambda x: x['inf'], reverse=True)
        network_top_25 = network_list[:25]
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

        # Export to CSV (will then send report with CSV links)
        csv_urls = {}
        try:
            # Ensure output directory exists (especially in GitHub Actions)
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            
            timestamp_str = datetime.now(LOCAL_TIMEZONE).strftime('%Y%m%d_%H%M%S')
            
            # 1. Store-Level Details CSV
            store_csv_path = os.path.join(OUTPUT_DIR, f'inf_store_details_{timestamp_str}.csv')
            store_fieldnames = [
                'timestamp', 'store_name', 'store_number', 'sku', 'product_name', 
                'inf_count', 'inf_rate', 'image_url', 'price', 'barcode',
                'stock_on_hand', 'stock_unit', 'stock_last_updated',
                'std_location', 'promo_location', 'product_status', 'commercially_active'
            ]
            
            with open(store_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f, fieldnames=store_fieldnames, extrasaction='ignore', quoting=csv.QUOTE_ALL
                )
                writer.writeheader()
                
                for store_name, store_number, items, inf_rate in results_list:
                    for item in items:
                        row = {
                            'timestamp': datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'),
                            'store_name': store_name,
                            'store_number': store_number or '',
                            'sku': item.get('sku', ''),
                            'product_name': item.get('name', ''),
                            'inf_count': item.get('inf', 0),
                            'inf_rate': inf_rate if inf_rate != 'N/A' else '',
                            'image_url': item.get('image_url', ''),
                            'price': item.get('price', ''),
                            'barcode': item.get('barcode', ''),
                            'stock_on_hand': item.get('stock_on_hand', ''),
                            'stock_unit': item.get('stock_unit', ''),
                            'stock_last_updated': item.get('stock_last_updated', ''),
                            'std_location': item.get('std_location', ''),
                            'promo_location': item.get('promo_location', ''),
                            'product_status': item.get('product_status', ''),
                            'commercially_active': item.get('commercially_active', '')
                        }
                        row = {key: sanitize_csv_value(value) for key, value in row.items()}
                        writer.writerow(row)
            
            app_logger.info(f"Store-level CSV exported to: {store_csv_path}")
            
            # 2. Network-Wide Summary CSV
            network_csv_path = os.path.join(OUTPUT_DIR, f'inf_network_summary_{timestamp_str}.csv')
            network_fieldnames = [
                'timestamp', 'rank', 'sku', 'product_name', 'total_inf_count',
                'store_count', 'top_contributing_stores', 'all_impacted_stores', 'image_url', 'price', 'barcode'
            ]
            
            with open(network_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f, fieldnames=network_fieldnames, extrasaction='ignore', quoting=csv.QUOTE_ALL
                )
                writer.writeheader()
                
                for rank, item in enumerate(network_top_25, 1):
                    # Format top contributing stores as "Store1 (count), Store2 (count), ..."
                    # Note: top_stores is now (store_name, inf_count, store_number)
                    top_stores_str = ', '.join([
                        f"{sanitize_store_name(store, STORE_PREFIX_RE)} ({count})"
                        for store, count, _ in item['top_stores']
                    ])

                    all_stores_str = ', '.join([
                        f"{sanitize_store_name(store, STORE_PREFIX_RE)} ({count})"
                        for store, count, _ in item.get('all_stores', [])
                    ])

                    row = {
                        'timestamp': datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'),
                        'rank': rank,
                        'sku': item.get('sku', ''),
                        'product_name': item.get('name', ''),
                        'total_inf_count': item.get('inf', 0),
                        'store_count': item.get('store_count', 0),
                        'top_contributing_stores': top_stores_str,
                        'all_impacted_stores': all_stores_str,
                        'image_url': item.get('image_url', ''),
                        'price': item.get('price', ''),
                        'barcode': item.get('barcode', '')
                    }
                    row = {key: sanitize_csv_value(value) for key, value in row.items()}
                    writer.writerow(row)
            
            app_logger.info(f"Network summary CSV exported to: {network_csv_path}")
            
            # Upload to GitHub Gist if running in GitHub Actions
            store_details_url = upload_csv_to_gist(
                store_csv_path, 
                f"INF Store Details - {datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M')}"
            )
            network_summary_url = upload_csv_to_gist(
                network_csv_path,
                f"INF Network Summary - {datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M')}"
            )
            
            # Store URLs if available
            if store_details_url:
                csv_urls['store_details'] = store_details_url
            if network_summary_url:
                csv_urls['network_summary'] = network_summary_url
            
        except Exception as e:
            app_logger.error(f"Error exporting CSV files: {e}")
        
        # Send Report - skip network-wide report if called from main scraper with specific stores
        # (top_n is already defined earlier in this function)
        await send_inf_report(results_list, network_top_10, skip_network_report=skip_network, title_prefix=title_prefix, top_n=top_n, csv_urls=csv_urls if csv_urls else None)

    finally:
        # Always try to post the quick actions card when applicable so users see buttons even if earlier steps hiccuped
        if should_post_quick_actions:
            try:
                from webhook import post_quick_actions_card
                app_logger.info("Posting quick actions card for follow-up workflows")
                await post_quick_actions_card(CHAT_WEBHOOK_URL, apps_script_url, DEBUG_MODE, app_logger)
            except Exception as quick_actions_error:
                app_logger.error(f"Failed to post quick actions card: {quick_actions_error}", exc_info=DEBUG_MODE)
        elif not apps_script_url:
            app_logger.info("Skipping quick actions card because apps_script_webhook_url is not configured")

        if local_playwright:
            if browser: await browser.close()
            await local_playwright.stop()

async def main():
    import argparse
    
    # CLI Argument Parsing
    parser = argparse.ArgumentParser(description='INF Scraper (Standalone)')
    parser.add_argument('--date-mode', choices=['today', 'yesterday', 'last_7_days', 'last_30_days', 'week_to_date', 'relative', 'custom'], help='Date range mode')
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
