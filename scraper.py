# =======================================================================================
#               AMAZON SELLER CENTRAL SCRAPER (CI/CD / COMMAND-LINE VERSION)
# =======================================================================================
# Refactored modular version - main orchestration and entry point
# =======================================================================================

import logging
import json
import asyncio
from asyncio import Queue
from threading import Lock
from typing import Dict, List
import re
import os
import argparse
from datetime import datetime
from pytz import timezone
from playwright.async_api import async_playwright, Browser

# Import our modules
from utils import setup_logging, sanitize_store_name, _save_screenshot, load_default_data, ensure_storage_state, LOCAL_TIMEZONE
from auth import check_if_login_needed, perform_login_and_otp, prime_master_session
from date_range import get_date_time_range_from_config, apply_date_time_range
from webhook import (post_to_chat_webhook, post_job_summary, post_performance_highlights,
                    post_quick_actions_card, add_to_pending_chat, flush_pending_chat_entries, log_submission)
from workers import auto_concurrency_manager, http_form_submitter_worker, process_single_store, worker_task
from inf_scraper import run_inf_analysis

#######################################################################
#                             APP SETUP & LOGGING
#######################################################################

app_logger = setup_logging()

#######################################################################
#                            CONFIG & CONSTANTS
#######################################################################

try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    app_logger.critical("config.json not found. Please create it before running.")
    exit(1)
except json.JSONDecodeError:
    app_logger.critical("config.json is not valid JSON. Please fix it.")
    exit(1)

# --- CLI Argument Parsing ---
parser = argparse.ArgumentParser(description='Amazon Seller Central Scraper')
parser.add_argument('--date-mode', choices=['today', 'yesterday', 'last_7_days', 'last_30_days', 'relative', 'custom'], help='Date range mode')
parser.add_argument('--start-date', help='Start date (MM/DD/YYYY)')
parser.add_argument('--end-date', help='End date (MM/DD/YYYY)')
parser.add_argument('--start-time', help='Start time (e.g., "12:00 AM")')
parser.add_argument('--end-time', help='End time (e.g., "11:59 PM")')
parser.add_argument('--relative-days', type=int, help='Days offset for relative mode')
parser.add_argument('--inf-mode', choices=['top10', 'all'], default='top10', help='INF analysis mode: top10 worst stores or all stores')
parser.add_argument('--inf-only', action='store_true', help='Run ONLY INF analysis for all stores, skipping dashboard metrics')
parser.add_argument('--top-n', type=int, default=5, help='Number of top INF items to show per store (5, 10, or 25)')

args, unknown = parser.parse_known_args()

# Merge CLI args into config (CLI takes precedence)
if args.date_mode:
    config['use_date_range'] = True
    config['date_range_mode'] = args.date_mode

if args.start_date: config['custom_start_date'] = args.start_date
if args.end_date: config['custom_end_date'] = args.end_date
if args.start_time: config['custom_start_time'] = args.start_time
if args.end_time: config['custom_end_time'] = args.end_time
if args.relative_days is not None: config['relative_days'] = args.relative_days
config['top_n_items'] = args.top_n  # Store top_n in config for INF scraper

# If start/end dates are provided via CLI, force mode to 'custom' if not specified
if (args.start_date or args.end_date) and not args.date_mode:
    config['use_date_range'] = True
    config['date_range_mode'] = 'custom'

DEBUG_MODE      = config.get('debug', False)
LOGIN_URL       = config['login_url']
CHAT_WEBHOOK_URL = config.get('chat_webhook_url')
STORE_WEBHOOK_URL = config.get('store_webhook_url') or CHAT_WEBHOOK_URL
PERFORMANCE_WEBHOOK_URL = config.get('performance_webhook_url') or CHAT_WEBHOOK_URL
APPS_SCRIPT_URL = config.get('apps_script_webhook_url')  # Optional - for interactive buttons
CHAT_BATCH_SIZE  = config.get('chat_batch_size', 100)
STORE_PREFIX_RE  = re.compile(r"^morrisons\s*-\s*", re.I)

# --- Constants for target-based emojis ---
EMOJI_GREEN_CHECK = "\u2705" # ✅
EMOJI_RED_CROSS = "\u274C"   # ❌
UPH_THRESHOLD = 80
LATES_THRESHOLD = 3.0
INF_THRESHOLD = 2.0

DEFAULT_FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLScg_jnxbuJsPs4KejUaVuu-HfMQKA3vSXZkWaYh-P_lbjE56A/formResponse"
CUSTOM_DATE_FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLSdCenFoO8cJFf8VU2-fS6TaFhwN_arTvAYDQQSsQ_aH1so09A/formResponse"

# Select URL based on whether date range is being used
if config.get('use_date_range', False):
    FORM_POST_URL = CUSTOM_DATE_FORM_URL
    app_logger.info(f"Using CUSTOM date range form URL: {FORM_POST_URL}")
else:
    FORM_POST_URL = DEFAULT_FORM_URL
    app_logger.info(f"Using DEFAULT form URL: {FORM_POST_URL}")

FIELD_MAP = {
    'store':          'entry.117918617',
    'orders':         'entry.128719511',
    'units':          'entry.66444552',
    'fulfilled':      'entry.2093280675',
    'uph':            'entry.316694141',
    'inf':            'entry.909185879',
    'found':          'entry.637588300',
    'cancelled':      'entry.1775576921',
    'lates':          'entry.2130893076',
    'time_available': 'entry.1823671734',
}

INITIAL_CONCURRENCY = config.get('initial_concurrency', 30)
NUM_FORM_SUBMITTERS = config.get('num_form_submitters', 2)

AUTO_CONF = config.get('auto_concurrency', {})
AUTO_ENABLED = AUTO_CONF.get('enabled', False)
AUTO_MIN_CONCURRENCY = AUTO_CONF.get('min_concurrency', config.get('min_concurrency', 1))
AUTO_MAX_CONCURRENCY = AUTO_CONF.get('max_concurrency', config.get('max_concurrency', INITIAL_CONCURRENCY))
CPU_UPPER_THRESHOLD = AUTO_CONF.get('cpu_upper_threshold', 90)
CPU_LOWER_THRESHOLD = AUTO_CONF.get('cpu_lower_threshold', 65)
MEM_UPPER_THRESHOLD = AUTO_CONF.get('mem_upper_threshold', 90)
CHECK_INTERVAL = AUTO_CONF.get('check_interval_seconds', 5)
COOLDOWN_SECONDS = AUTO_CONF.get('cooldown_seconds', 15)

LOG_FILE        = os.path.join('output', 'submissions.log')
JSON_LOG_FILE   = os.path.join('output', 'submissions.jsonl')
STORAGE_STATE   = 'state.json'
OUTPUT_DIR      = 'output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_TIMEOUT    = config.get('page_timeout_ms', 30000)
WAIT_TIMEOUT    = config.get('element_wait_timeout_ms', 10000)
ACTION_TIMEOUT = int(PAGE_TIMEOUT / 2)
WORKER_RETRY_COUNT = 3

RESOURCE_BLOCKLIST = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "adservice.google.com", "facebook.net", "fbcdn.net", "analytics.tiktok.com",
]

#######################################################################
#                      GLOBALS
#######################################################################

log_lock      = asyncio.Lock()
progress_lock = Lock()
urls_data     = []
progress      = {"current": 0, "total": 0, "lastUpdate": "N/A"}
run_failures  = []
start_time    = None
failure_timestamps = []
failure_lock = asyncio.Lock()

# Metrics for Advanced Reporting
metrics = {
    "collection_times": [],
    "submission_times": [],
    "retries": 0,
    "total_orders": 0,
    "total_units": 0,
    "retry_stores": set()
}
metrics_lock = asyncio.Lock()

pending_chat_entries: List[Dict[str, str]] = []
pending_chat_lock = asyncio.Lock()
chat_batch_count = 0

# Track all submitted store data for performance highlights
submitted_store_data: List[Dict[str, str]] = []
submitted_data_lock = asyncio.Lock()

playwright = None
browser = None

# Use dict references for mutable state in workers
concurrency_limit_ref = {'value': INITIAL_CONCURRENCY}
active_workers_ref = {'value': 0}
concurrency_condition = asyncio.Condition()
last_concurrency_change_ref = {'value': 0.0}

#######################################################################
#                  MAIN PROCESS LOOP & ORCHESTRATION
#######################################################################

async def process_urls():
    global progress, start_time, run_failures, browser, chat_batch_count
    
    pool_size = config.get('initial_concurrency', 30)
    app_logger.info(f"Job 'process_urls' started with Worker Pool size: {pool_size}")
    run_failures = []
    
    load_default_data(urls_data, app_logger)
    if not urls_data:
        app_logger.error("No URLs to process. Aborting job.")
        return

    login_is_required = True
    if ensure_storage_state(STORAGE_STATE, app_logger):
        app_logger.info("Existing auth state file found. Verifying session is still active...")
        temp_context = None
        try:
            first_store = urls_data[0]
            test_dash_url = f"https://sellercentral.amazon.co.uk/snowdash?ref_=mp_home_logo_xx&cor=mmp_EU&mons_sel_dir_mcid={first_store['merchant_id']}&mons_sel_mkid={first_store['marketplace_id']}"
            with open(STORAGE_STATE) as f: storage_for_check = json.load(f)
            temp_context = await browser.new_context(storage_state=storage_for_check)
            temp_page = await temp_context.new_page()
            if not await check_if_login_needed(temp_page, test_dash_url, PAGE_TIMEOUT, DEBUG_MODE, app_logger):
                app_logger.info("Session verification successful. Skipping login.")
                login_is_required = False
            else:
                app_logger.warning("Session has expired or is invalid. A new login is required.")
        except Exception as e:
            app_logger.error(f"An error occurred during session verification. Forcing re-login. Error: {e}", exc_info=DEBUG_MODE)
        finally:
            if temp_context: await temp_context.close()
    else:
        app_logger.info("No existing auth state file found. Login is required.")

    if login_is_required:
        MAX_LOGIN_ATTEMPTS = 3
        login_successful = False
        
        async def perform_login_wrapper(page):
            return await perform_login_and_otp(page, LOGIN_URL, config, PAGE_TIMEOUT, DEBUG_MODE, app_logger,
                                              lambda p, prefix: _save_screenshot(p, prefix, OUTPUT_DIR, LOCAL_TIMEZONE, app_logger))
        
        for attempt in range(MAX_LOGIN_ATTEMPTS):
            app_logger.info(f"Attempting to prime a new master session (Attempt {attempt + 1}/{MAX_LOGIN_ATTEMPTS})...")
            if await prime_master_session(browser, STORAGE_STATE, PAGE_TIMEOUT, ACTION_TIMEOUT, perform_login_wrapper, app_logger):
                login_successful = True
                break
            if attempt < MAX_LOGIN_ATTEMPTS - 1:
                app_logger.warning(f"Session priming failed on attempt {attempt + 1}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
        
        if not login_successful:
            app_logger.critical(f"Critical: Session priming failed after {MAX_LOGIN_ATTEMPTS} attempts. Aborting job.")
            return

    if args.inf_only:
        app_logger.info("INF ONLY mode enabled. Skipping dashboard scraping.")
        # Pass None for target_stores so the full network summary and quick actions are included
        # The INF scraper will load stores internally when target_stores is None
        await run_inf_analysis(None, browser, config)
        return

    with open(STORAGE_STATE) as f: storage_template = json.load(f)
    
    # Queues
    job_queue = Queue()
    submission_queue = Queue()
    
    # Populate Job Queue
    for store in urls_data:
        job_queue.put_nowait(store)
        
    with progress_lock: 
        progress = {"current": 0, "total": len(urls_data), "lastUpdate": "N/A"}
    
    start_time = datetime.now(LOCAL_TIMEZONE)

    # Create wrapper functions for workers
    def get_date_range():
        return get_date_time_range_from_config(config, LOCAL_TIMEZONE, app_logger)
    
    def sanitize_wrapper(name):
        return sanitize_store_name(name, STORE_PREFIX_RE)
    
    async def post_webhook_wrapper(entries):
        global chat_batch_count
        chat_batch_count += 1
        await post_to_chat_webhook(entries, STORE_WEBHOOK_URL, chat_batch_count, get_date_range,
                                   sanitize_wrapper, UPH_THRESHOLD, LATES_THRESHOLD, INF_THRESHOLD,
                                   EMOJI_GREEN_CHECK, EMOJI_RED_CROSS, LOCAL_TIMEZONE, DEBUG_MODE, app_logger)
    
    async def add_chat_wrapper(entry):
        await add_to_pending_chat(entry, STORE_WEBHOOK_URL, pending_chat_lock, pending_chat_entries,
                                  CHAT_BATCH_SIZE, post_webhook_wrapper)
    
    async def log_submission_wrapper(data):
        await log_submission(data, log_lock, LOG_FILE, JSON_LOG_FILE, submitted_data_lock,
                            submitted_store_data, add_chat_wrapper, LOCAL_TIMEZONE, app_logger)
    
    async def apply_date_range_wrapper(page, store_name):
        return await apply_date_time_range(page, store_name, get_date_range, ACTION_TIMEOUT, DEBUG_MODE, app_logger)
    
    async def process_store_wrapper(context, store_info, queue):
        await process_single_store(context, store_info, queue, WORKER_RETRY_COUNT, RESOURCE_BLOCKLIST,
                                   apply_date_range_wrapper, WAIT_TIMEOUT, ACTION_TIMEOUT,
                                   metrics_lock, metrics, run_failures, failure_lock, failure_timestamps,
                                   DEBUG_MODE, app_logger)
    
    # Start Auto-concurrency Manager
    if AUTO_ENABLED:
        asyncio.create_task(auto_concurrency_manager(
            concurrency_limit_ref, last_concurrency_change_ref, AUTO_ENABLED, AUTO_MIN_CONCURRENCY,
            AUTO_MAX_CONCURRENCY, CPU_UPPER_THRESHOLD, CPU_LOWER_THRESHOLD, MEM_UPPER_THRESHOLD,
            CHECK_INTERVAL, COOLDOWN_SECONDS, failure_lock, failure_timestamps,
            concurrency_condition, app_logger
        ))

    # Start Form Submitters
    app_logger.info(f"Starting {NUM_FORM_SUBMITTERS} HTTP form submitter worker(s).")
    form_submitter_tasks = [
        asyncio.create_task(http_form_submitter_worker(
            submission_queue, i + 1, FORM_POST_URL, FIELD_MAP, log_submission_wrapper,
            progress_lock, progress, metrics_lock, metrics, run_failures, LOCAL_TIMEZONE,
            DEBUG_MODE, app_logger
        ))
        for i in range(NUM_FORM_SUBMITTERS)
    ]
    
    # Start Worker Pool
    app_logger.info(f"Spinning up {pool_size} browser workers...")
    workers = [
        asyncio.create_task(worker_task(
            i+1, browser, storage_template, job_queue, submission_queue, PAGE_TIMEOUT, ACTION_TIMEOUT,
            process_store_wrapper, active_workers_ref, concurrency_limit_ref,
            concurrency_condition, app_logger
        ))
        for i in range(pool_size)
    ]
    
    # Wait for all jobs to be processed
    await asyncio.gather(*workers)
    
    app_logger.info("All workers finished. Waiting for submission queue to empty...")
    await submission_queue.join()
    
    async def flush_wrapper():
        await flush_pending_chat_entries(STORE_WEBHOOK_URL, pending_chat_lock, pending_chat_entries, post_webhook_wrapper)
    
    await flush_wrapper()
    
    app_logger.info("Cancelling form submitter workers...")
    for task in form_submitter_tasks: task.cancel()
    await asyncio.gather(*form_submitter_tasks, return_exceptions=True)

    elapsed = (datetime.now(LOCAL_TIMEZONE) - start_time).total_seconds()
    app_logger.info(f"Processing finished. Processed {progress['current']}/{progress['total']} in {elapsed:.2f}s")
    
    # Send Job Summary
    await post_job_summary(progress['total'], progress['current'], run_failures, elapsed,
                          PERFORMANCE_WEBHOOK_URL, metrics_lock, metrics, LOCAL_TIMEZONE, DEBUG_MODE, app_logger,
                          APPS_SCRIPT_URL)
    
    # Send Performance Highlights & Trigger INF Deep Dive
    async with submitted_data_lock:
        if submitted_store_data:
            # 1. Send Performance Highlights
            await post_performance_highlights(submitted_store_data, PERFORMANCE_WEBHOOK_URL, sanitize_wrapper,
                                             LOCAL_TIMEZONE, DEBUG_MODE, app_logger, APPS_SCRIPT_URL)
            
            # 2. Identify Bottom 5 INF Stores for Deep Dive
            app_logger.info("Identifying bottom 5 INF stores for deep dive analysis...")
            try:
                # Create a lookup for full store details
                store_lookup = {s['store_name']: s for s in urls_data}
                
                # Parse INF and sort
                def parse_inf(item):
                    try:
                        return float(item.get('inf', '0').replace('%', '').strip())
                    except:
                        return -1.0

                # Filter for stores that actually have data and exist in lookup
                # Also exclude stores with 0 orders if desired, but for INF, high INF on low orders is still bad.
                valid_stores = [s for s in submitted_store_data if s.get('store') in store_lookup]
                
                # Sort by INF descending (Higher INF is worse)
                sorted_by_inf = sorted(valid_stores, key=parse_inf, reverse=True)
                
                # Take top 10 or all based on mode
                if args.inf_mode == 'all':
                    target_stores_inf_list = sorted_by_inf
                    app_logger.info(f"INF Mode: ALL. Targeting {len(target_stores_inf_list)} stores.")
                else:
                    target_stores_inf_list = sorted_by_inf[:10]
                    app_logger.info(f"INF Mode: Top 10. Targeting {len(target_stores_inf_list)} stores.")
                
                target_stores_for_inf = []
                for s in target_stores_inf_list:
                    full_details = store_lookup.get(s['store'])
                    if full_details:
                        # Create a copy to avoid modifying the original urls_data
                        store_with_inf = full_details.copy()
                        store_with_inf['inf_rate'] = s.get('inf', 'N/A')
                        target_stores_for_inf.append(store_with_inf)
                
                if target_stores_for_inf:
                    app_logger.info(f"Triggering INF analysis for {len(target_stores_for_inf)} stores: {[s['store_name'] for s in target_stores_for_inf]}")
                    # Run INF analysis using the existing browser
                    await run_inf_analysis(target_stores_for_inf, browser, config)
                else:
                    app_logger.info("No stores found for INF analysis.")
                    
            except Exception as e:
                app_logger.error(f"Failed to run INF analysis: {e}", exc_info=True)

            submitted_store_data.clear()

    # Send Quick Actions card last so buttons are always at the bottom of the thread
    await post_quick_actions_card(PERFORMANCE_WEBHOOK_URL, APPS_SCRIPT_URL, DEBUG_MODE, app_logger)

    if run_failures:
        app_logger.warning(f"Completed with {len(run_failures)} issue(s): {', '.join(run_failures)}")
    else:
        app_logger.info("Completed successfully.")


#######################################################################
#                         MAIN EXECUTION BLOCK
#######################################################################

async def main():
    global playwright, browser
    app_logger.info("Starting up in single-run mode...")
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=not DEBUG_MODE,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-accelerated-2d-canvas",
                "--disable-gl-drawing-for-tests",
            ]
        )
        app_logger.info("Browser launched successfully.")
        await process_urls()
    except Exception as e:
        app_logger.critical(f"A critical error occurred in the main execution block: {e}", exc_info=True)
    finally:
        app_logger.info("Task finished. Initiating shutdown...")
        if browser and browser.is_connected():
            await browser.close()
            app_logger.info("Browser instance closed.")
        if playwright:
            await playwright.stop()
            app_logger.info("Playwright stopped.")
        app_logger.info("Run complete.")

if __name__ == "__main__":
    asyncio.run(main())
