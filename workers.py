# =======================================================================================
#                    WORKERS MODULE - Worker Tasks & Concurrent Processing
# =======================================================================================

import asyncio
import aiohttp
import ssl
import certifi
import re
import psutil
from asyncio import Queue
from playwright.async_api import BrowserContext, Browser, Page, TimeoutError, expect
from typing import Dict
from datetime import datetime


async def auto_concurrency_manager(concurrency_limit_ref: dict, last_change_ref: dict,
                                   auto_enabled: bool, auto_min: int, auto_max: int,
                                   cpu_upper: float, cpu_lower: float, mem_upper: float,
                                   check_interval: int, cooldown: int,
                                   failure_lock, failure_timestamps: list,
                                   concurrency_condition, app_logger):
    """Manages automatic concurrency scaling based on system resources and failure rate."""
    if not auto_enabled:
        return
    app_logger.info(f"Auto-concurrency enabled with range {auto_min}-{auto_max}")
    
    while True:
        now = asyncio.get_event_loop().time()
        
        # 1. Check Failure Rate (Error-Aware Scaling)
        async with failure_lock:
            while failure_timestamps and now - failure_timestamps[0] > 60:
                failure_timestamps.pop(0)
            recent_failure_count = len(failure_timestamps)
        
        estimated_throughput = concurrency_limit_ref['value'] * 30 
        failure_rate = recent_failure_count / max(estimated_throughput, 1)
        
        if failure_rate > 0.05: # >5% failure rate
            if now - last_change_ref['value'] >= cooldown:
                concurrency_limit_ref['value'] = max(auto_min, int(concurrency_limit_ref['value'] * 0.5))
                last_change_ref['value'] = now
                app_logger.warning(f"Auto-concurrency: THROTTLING DOWN to {concurrency_limit_ref['value']} due to high failure rate ({failure_rate:.1%})")
                async with concurrency_condition:
                    concurrency_condition.notify_all()
                await asyncio.sleep(cooldown * 2)
                continue

        # 2. Standard Resource Scaling
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent
        
        if now - last_change_ref['value'] >= cooldown:
            if (cpu > cpu_upper or mem > mem_upper) and concurrency_limit_ref['value'] > auto_min:
                concurrency_limit_ref['value'] -= 1
                last_change_ref['value'] = now
                app_logger.info(f"Auto-concurrency: decreased to {concurrency_limit_ref['value']} (CPU {cpu:.1f}%, MEM {mem:.1f}%)")
            elif cpu < cpu_lower and mem < mem_upper and concurrency_limit_ref['value'] < auto_max:
                concurrency_limit_ref['value'] += 1
                last_change_ref['value'] = now
                app_logger.info(f"Auto-concurrency: increased to {concurrency_limit_ref['value']} (CPU {cpu:.1f}%, MEM {mem:.1f}%)")
            
            if concurrency_limit_ref['value'] > auto_max:
                concurrency_limit_ref['value'] = auto_max
            if concurrency_limit_ref['value'] < auto_min:
                concurrency_limit_ref['value'] = auto_min
            async with concurrency_condition:
                concurrency_condition.notify_all()
        await asyncio.sleep(check_interval)


async def http_form_submitter_worker(queue: Queue, worker_id: int, form_post_url: str,
                                     field_map: dict, log_submission_func, progress_lock,
                                     progress: dict, metrics_lock, metrics: dict,
                                     run_failures: list, local_timezone,
                                     debug_mode: bool, app_logger):
    """Worker that submits form data to Google Forms via HTTP POST."""
    log_prefix = f"[HTTP-Submitter-{worker_id}]"
    app_logger.info(f"{log_prefix} Starting up...")
    timeout = aiohttp.ClientTimeout(total=20)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            form_data = None
            try:
                form_data = await queue.get()
                store_name = form_data.get('store', 'Unknown')
                
                # Map keys to Google Form entry IDs
                payload = {}
                for key, value in form_data.items():
                    if key in field_map:
                        payload[field_map[key]] = value
                
                submit_start = asyncio.get_event_loop().time()
                async with session.post(form_post_url, data=payload, timeout=10) as resp:
                    if resp.status == 200:
                        await log_submission_func(form_data)
                        app_logger.info(f"{log_prefix} Submitted data for {form_data.get('store', 'Unknown')}")
                        with progress_lock:
                            progress["current"] += 1
                            progress["lastUpdate"] = datetime.now(local_timezone).strftime("%H:%M:%S")
                        
                        submit_duration = asyncio.get_event_loop().time() - submit_start
                        async with metrics_lock:
                            metrics["submission_times"].append((form_data.get('store', 'Unknown'), submit_duration))
                    else:
                        error_text = await resp.text()
                        app_logger.error(f"{log_prefix} Submission for {store_name} failed. Status: {resp.status}. Response: {error_text[:200]}")
                        run_failures.append(f"{store_name} (HTTP Submit Fail {resp.status})")
            except asyncio.CancelledError:
                break
            except Exception as e:
                failed_store = form_data.get('store', 'Unknown') if form_data else "Unknown"
                app_logger.error(f"{log_prefix} Unhandled exception for {failed_store}: {e}", exc_info=debug_mode)
                run_failures.append(f"{failed_store} (Submit Exception)")
            finally:
                if form_data:
                    queue.task_done()
    app_logger.info(f"{log_prefix} Shut down.")


async def process_single_store(context: BrowserContext, store_info: Dict[str,str], queue: Queue,
                               worker_retry_count: int, resource_blocklist: list,
                               apply_date_range_func, wait_timeout: int, action_timeout: int,
                               metrics_lock, metrics: dict, run_failures: list,
                               failure_lock, failure_timestamps: list,
                               debug_mode: bool, app_logger):
    """Process a single store: navigate, scrape metrics, queue for form submission."""
    start_ts = asyncio.get_event_loop().time()
    merchant_id = store_info['merchant_id']
    store_name  = store_info['store_name']
    
    for attempt in range(worker_retry_count):
        page = None
        try:
            marketplace_id = store_info['marketplace_id']
            if not marketplace_id:
                app_logger.error(f"Skipping {store_name}: marketplace_id is missing.")
                run_failures.append(f"{store_name} (Missing MKID)")
                return

            page = await context.new_page()
            
            # Block resources on this page
            async def block_resources(route):
                if (any(domain in route.request.url for domain in resource_blocklist) or
                        route.request.resource_type in ("image", "stylesheet", "font", "media")):
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", block_resources)

            dash_url = f"https://sellercentral.amazon.co.uk/snowdash?ref_=mp_home_logo_xx&cor=mmp_EU&mons_sel_dir_mcid={merchant_id}&mons_sel_mkid={marketplace_id}"
            await page.goto(dash_url, timeout=30000, wait_until="domcontentloaded")
            
            refresh_button_selector = "#content > div > div.mainAppContainerExternal > div.css-6pahkd.action-bar-container > div > div.filterbar-right-slot > kat-button:nth-child(2) > button"
            METRICS_TIMEOUT = 45_000
            
            # Wait for dashboard to be ready
            refresh_button = page.locator(refresh_button_selector)
            await expect(refresh_button).to_be_visible(timeout=wait_timeout)
            app_logger.info(f"[{store_name}] Dashboard loaded, refresh button visible")
            
            # Apply date/time range filter if configured
            date_range_applied = await apply_date_range_func(page, store_name)
            if not date_range_applied:
                app_logger.warning(f"[{store_name}] Proceeding without date range filter")
            
            # Wait for metrics response after clicking refresh
            async with page.expect_response(lambda r: "summationMetrics" in r.url and r.status == 200, timeout=METRICS_TIMEOUT) as resp_info:
                await refresh_button.click()
            
            response = await resp_info.value
            api_data = await response.json()

            formatted_lates = "0 %"
            try:
                header_second_row = page.locator("kat-table-head kat-table-row").nth(1)
                lates_cell = header_second_row.locator("kat-table-cell").nth(10)
                await expect(lates_cell).to_be_visible(timeout=10000)
                cell_text = (await lates_cell.text_content() or "").strip()
                app_logger.info(f"[{store_name}] Raw 'Lates' text scraped: '{cell_text}'")

                if re.fullmatch(r"\d+(\.\d+)?\s*%", cell_text):
                    formatted_lates = cell_text
                    app_logger.info(f"[{store_name}] Successfully parsed 'Lates' as: {formatted_lates}")
                elif cell_text:
                    app_logger.warning(f"[{store_name}] Scraped 'Lates' value '{cell_text}' but it didn't match format, defaulting to 0 %.")
                else:
                    app_logger.warning(f"[{store_name}] 'Lates' cell was visible but empty, defaulting to 0 %.")

            except TimeoutError:
                app_logger.warning(f"[{store_name}] Timed out waiting for the 'Lates' cell to become visible, defaulting to 0 %.")
            except Exception as e:
                app_logger.error(f"[{store_name}] An unexpected error occurred while scraping 'Lates': {e}", exc_info=debug_mode)

            milliseconds_from_api = float(api_data.get('TimeAvailable_V2', 0.0))
            total_seconds = int(milliseconds_from_api / 1000)
            total_minutes, _ = divmod(abs(total_seconds), 60)
            total_hours, remaining_minutes = divmod(total_minutes, 60)
            formatted_time_available = f"{total_hours}:{remaining_minutes:02d}"

            form_data = {
                'store': store_name, 'orders': str(api_data.get('OrdersShopped_V2', 0)),
                'units': str(api_data.get('RequestedQuantity_V2', 0)), 'fulfilled': str(api_data.get('PickedUnits_V2', 0)),
                'uph': f"{api_data.get('AverageUPH_V2', 0.0):.0f}", 'inf': f"{api_data.get('ItemNotFoundRate_V2', 0.0):.1f} %",
                'found': f"{api_data.get('ItemFoundRate_V2', 0.0):.1f} %", 'cancelled': str(api_data.get('ShortedUnits_V2', 0)),
                'lates': formatted_lates, 'time_available': formatted_time_available
            }
            await queue.put(form_data)
            
            duration = asyncio.get_event_loop().time() - start_ts
            async with metrics_lock:
                metrics["collection_times"].append((store_name, duration))
                metrics["total_orders"] += int(api_data.get('OrdersShopped_V2', 0))
                metrics["total_units"] += int(api_data.get('RequestedQuantity_V2', 0))
            
            app_logger.info(f"[{store_name}] Data collection complete ({duration:.2f}s).")
            return # Success, exit loop

        except Exception as e:
            app_logger.warning(f"[{store_name}] Failed attempt {attempt + 1}: {e}")
            if attempt < worker_retry_count - 1:
                async with metrics_lock:
                    metrics["retries"] += 1
                    metrics["retry_stores"].add(store_name)
                sleep_time = 2 ** attempt
                app_logger.info(f"[{store_name}] Retrying in {sleep_time}s...")
                await asyncio.sleep(sleep_time)
            else:
                run_failures.append(f"{store_name} (Fail)")
                async with failure_lock:
                    failure_timestamps.append(asyncio.get_event_loop().time())
        finally:
            if page: await page.close()


async def worker_task(worker_id: int, browser: Browser, storage_template: Dict, job_queue: Queue, 
                     submission_queue: Queue, page_timeout: int, action_timeout: int,
                     process_store_func, active_workers_ref: dict, concurrency_limit_ref: dict,
                     concurrency_condition, app_logger):
    """Main worker task that processes stores from the job queue."""
    app_logger.info(f"[Worker-{worker_id}] Starting up.")
    context = None
    try:
        context = await browser.new_context(storage_state=storage_template)
        context.set_default_navigation_timeout(page_timeout)
        context.set_default_timeout(action_timeout)
        
        while True:
            try:
                store_item = job_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            # Enforce Concurrency Limit
            async with concurrency_condition:
                while active_workers_ref['value'] >= concurrency_limit_ref['value']:
                    await concurrency_condition.wait()
                active_workers_ref['value'] += 1

            try:
                await process_store_func(context, store_item, submission_queue)
            finally:
                async with concurrency_condition:
                    active_workers_ref['value'] -= 1
                    concurrency_condition.notify_all()
                job_queue.task_done()
            
    except Exception as e:
        app_logger.error(f"[Worker-{worker_id}] Crashed: {e}")
    finally:
        if context: await context.close()
        app_logger.info(f"[Worker-{worker_id}] Shutting down.")
