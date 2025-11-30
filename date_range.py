# =======================================================================================
#                  DATE RANGE MODULE - Date/Time Range Selection Helpers
# =======================================================================================

from datetime import datetime, timedelta
from playwright.async_api import Page, TimeoutError, expect
import re
from pytz import timezone
from utils import sanitize_store_name, _save_screenshot

STORE_PREFIX_RE = re.compile(r"^morrisons\s*-\s*", re.I)


# CSS selectors for the "Customised" dashboard tab
CUSTOMISED_TAB_SELECTORS = [
    "span.auiViewOptionNotSelected:has-text(\"Customised\")",
    "span.auiViewOptionSelected:has-text(\"Customised\")", # In case it's already selected
    "#content > div > div.mainAppContainerExternal > div.paddingTop > div > div > div > div > span:nth-child(4)",
    "span:has-text(\"Customised\")",
    "[role='tab']:has-text(\"Customised\")",
]

# Selectors for the date range picker widget
DATE_PICKER_SELECTORS = [
    "kat-date-range-picker",
    "kat-dashboard-date-range-picker",
    "[class*='date-range-picker']",
    "[class*='dateRangePicker']",
    "[class*='date-picker']",
    "div:has(> input[type='text'][placeholder*='date' i])",
    "div:has(> input[type='text']) >> nth=0",  # First div containing text inputs
]


async def _find_customised_tab(page: Page, wait_timeout: int):
    """Return the first matching locator for the Customised dashboard tab."""
    for selector in CUSTOMISED_TAB_SELECTORS:
        locator = page.locator(selector)
        try:
            await expect(locator).to_be_visible(timeout=wait_timeout)
            return locator
        except AssertionError:
            continue
    raise AssertionError("Customised tab not found with known selectors")


async def _wait_for_date_picker(page: Page, wait_timeout: int):
    """Return the first matching date picker locator."""
    for selector in DATE_PICKER_SELECTORS:
        locator = page.locator(selector)
        try:
            await expect(locator).to_be_visible(timeout=wait_timeout)
            return locator
        except AssertionError:
            continue
    raise AssertionError("Date picker not found with known selectors")



async def _apply_custom_date_range(page: Page, store_name: str, date_range: dict, action_timeout: int, debug_mode: bool, app_logger) -> bool:
    """Helper to apply a custom date range using inputs."""
    app_logger.info(f"[{store_name}] Applying Customised range: {date_range['start_date']} {date_range['start_time']} to {date_range['end_date']} {date_range['end_time']}")
    
    # Click "Customised" tab
    try:
        await page.get_by_role("link", name="Customised").click()
    except:
        # Fallback to previous selectors if link role fails
        customised_tab = await _find_customised_tab(page, 5000)
        await customised_tab.click(timeout=action_timeout, force=True)
    
    app_logger.info(f"[{store_name}] Clicked 'Customised' tab")
    
    # Wait for the date picker widget to load
    try:
        # Try waiting for the specific IDs mentioned by user first
        await page.wait_for_selector("#startDate", state="visible", timeout=5000)
        app_logger.info(f"[{store_name}] Found #startDate input")
        
        # Helper to fill date
        async def fill_input_id(id_val, val):
            loc = page.locator(id_val)
            await loc.click()
            await loc.clear()
            await loc.type(val, delay=50)
            await loc.press('Enter')
        
        await fill_input_id("#startDate", date_range['start_date'])
        await fill_input_id("#endDate", date_range['end_date'])
        
    except TimeoutError:
        # Fallback to the previous logic (kat-date-range-picker)
        app_logger.info(f"[{store_name}] #startDate not found, trying kat-date-range-picker")
        
        await page.wait_for_selector("kat-date-range-picker", state="attached", timeout=5000)
        date_picker = await _wait_for_date_picker(page, 10000)
        
        # Define STORE_PREFIX_RE locally as requested
        STORE_PREFIX_RE = re.compile(r"^morrisons\s*-\s*", re.I)
        
        # Take a debug screenshot ONLY if debug_mode is True
        if debug_mode:
            await _save_screenshot(page, f"debug_datepicker_{sanitize_store_name(store_name, STORE_PREFIX_RE)}", "output", timezone('Europe/London'), app_logger)
        
        # Date inputs are type="text" within the date picker
        date_inputs = date_picker.locator('input[type="text"]')
        await expect(date_inputs.first).to_be_visible(timeout=5000)
        
        # Helper to robustly fill an input
        async def fill_date_input(locator, value):
            await locator.click()
            await locator.clear()
            await locator.type(value, delay=50)
            await locator.press('Enter')
            await locator.evaluate("(el, val) => { el.value = val; el.dispatchEvent(new Event('input', {bubbles: true})); el.dispatchEvent(new Event('change', {bubbles: true})); }", value)

        # Fill date fields
        await fill_date_input(date_inputs.nth(0), date_range['start_date'])
        await fill_date_input(date_inputs.nth(1), date_range['end_date'])
        
        await date_inputs.nth(1).blur()
        await page.wait_for_timeout(1000)
    
    # Final Apply Step (Common to both custom flows)
    apply_candidates = [
        page.get_by_role("button", name="Apply"),
        page.locator("button:has-text('Apply')"),
        page.locator(".apply-button"),
        page.locator("[type='submit']"),
        page.get_by_text("Apply", exact=True),
        page.get_by_role("button", name="Submit")
    ]
    
    apply_btn = None
    for candidate in apply_candidates:
        if await candidate.count() > 0 and await candidate.first.is_visible():
            apply_btn = candidate.first
            break
    
    if apply_btn:
        async with page.expect_response(
            lambda r: r.status == 200 and ("metrics" in r.url or "inventory" in r.url or "submit" in r.url or "dashboard" in r.url),
            timeout=30000
        ) as apply_info:
            await apply_btn.click(timeout=action_timeout)
        try:
            await apply_info.value
        except:
            pass
        return True
    else:
        app_logger.warning(f"[{store_name}] Could not find 'Apply' button")
        return False


def get_date_time_range_from_config(config: dict, local_timezone, app_logger) -> dict | None:
    """Calculate start/end dates and times based on configuration.
    
    Returns:
        dict with 'mode', 'start_date', 'end_date', 'start_time', 'end_time' or None if disabled
    """
    if not config.get('use_date_range', False):
        return None
    
    mode = config.get('date_range_mode', 'today')
    now = datetime.now(local_timezone)
    
    start_date = end_date = None
    start_time = end_time = None
    
    if mode in ['today', 'yesterday', 'last_7_days', 'last_30_days']:
        # These modes use built-in filters, no need to calculate dates for input
        pass
    
    elif mode == 'week_to_date':
        # Calculate Monday of current week to Today
        # weekday(): Monday is 0, Sunday is 6
        current_weekday = now.weekday()
        start_of_week = now - timedelta(days=current_weekday)
        
        start_date = start_of_week.strftime("%m/%d/%Y")
        end_date = now.strftime("%m/%d/%Y")
        start_time = "12:00 AM"
        end_time = "11:59 PM"
        
        # Map to custom for the applicator to use inputs
        mode = 'custom'

    elif mode == 'relative':
        days_offset = config.get('relative_days', 0)
        target_date = now + timedelta(days=days_offset)
        start_date = end_date = target_date.strftime("%m/%d/%Y")
        start_time = config.get('start_time', '12:00 AM')
        end_time = config.get('end_time', '11:59 PM')
        # Map relative to custom for the applicator if it's just a specific date, 
        # but if we want to use the 'Customised' tab, we keep mode as 'relative' or 'custom'
        mode = 'custom' 
    
    elif mode == 'custom':
        start_date = config.get('custom_start_date')
        end_date = config.get('custom_end_date')
        start_time = config.get('custom_start_time', '12:00 AM')
        end_time = config.get('custom_end_time', '11:59 PM')
        
        if not start_date or not end_date:
            app_logger.error("Custom date range mode requires 'custom_start_date' and 'custom_end_date' in config")
            return None
    
    else:
        app_logger.error(f"Unknown date_range_mode: {mode}")
        return None
    
    return {
        'mode': mode,
        'start_date': start_date,
        'end_date': end_date,
        'start_time': start_time,
        'end_time': end_time
    }


async def apply_date_time_range(page: Page, store_name: str, get_date_range_func, 
                                action_timeout: int, debug_mode: bool, app_logger) -> bool:
    """Apply date/time range filter if configured.
    
    Args:
        page: Playwright page object
        store_name: Store name for logging
        
    Returns:
        True if date range was applied successfully or not needed, False on error
    """
    date_range = get_date_range_func()
    if not date_range:
        app_logger.info(f"[{store_name}] Date range filtering disabled, using default view")
        return True
    
    mode = date_range.get('mode')
    
    try:
        if mode == 'today':
            app_logger.info(f"[{store_name}] Mode is 'today' (default view) - no action needed")
            return True
            
        elif mode == 'yesterday':
            app_logger.info(f"[{store_name}] Applying filter: Yesterday")
            await page.get_by_role("link", name="Yesterday").click()
            return True
            
        elif mode == 'last_7_days':
            app_logger.info(f"[{store_name}] Applying filter: Last 7 days")
            await page.get_by_role("link", name="Last 7 days").click()
            return True
            
        elif mode == 'last_30_days':
            app_logger.info(f"[{store_name}] Applying filter: Last 30 days")
            await page.get_by_role("link", name="Last 30 days", exact=True).click()
            return True
            
        elif mode == 'custom':
            return await _apply_custom_date_range(page, store_name, date_range, action_timeout, debug_mode, app_logger)

    except Exception as e:
        app_logger.error(f"[{store_name}] Error applying date range: {e}", exc_info=debug_mode)
        return False
    
    return True
