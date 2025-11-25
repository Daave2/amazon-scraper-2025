# =======================================================================================
#                  DATE RANGE MODULE - Date/Time Range Selection Helpers
# =======================================================================================

from datetime import datetime, timedelta
from playwright.async_api import Page, TimeoutError, expect


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


def get_date_time_range_from_config(config: dict, local_timezone, app_logger) -> dict | None:
    """Calculate start/end dates and times based on configuration.
    
    Returns:
        dict with 'start_date', 'end_date', 'start_time', 'end_time' or None if disabled
    """
    if not config.get('use_date_range', False):
        return None
    
    mode = config.get('date_range_mode', 'today')
    now = datetime.now(local_timezone)
    
    if mode == 'today':
        start_date = end_date = now.strftime("%m/%d/%Y")
        start_time = config.get('start_time', '12:00 AM')
        end_time = config.get('end_time', '11:59 PM')
    
    elif mode == 'relative':
        days_offset = config.get('relative_days', 0)
        target_date = now + timedelta(days=days_offset)
        start_date = end_date = target_date.strftime("%m/%d/%Y")
        start_time = config.get('start_time', '12:00 AM')
        end_time = config.get('end_time', '11:59 PM')
    
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
    
    try:
        app_logger.info(f"[{store_name}] Applying date/time range: {date_range['start_date']} {date_range['start_time']} to {date_range['end_date']} {date_range['end_time']}")
        
        # Step 1: Click the "Customised" tab
        customised_tab = await _find_customised_tab(page, 10000)
        await customised_tab.scroll_into_view_if_needed(timeout=action_timeout)
        # Wait briefly for tab to be clickable
        await page.wait_for_timeout(2000)
        await customised_tab.click(timeout=action_timeout, force=True)
        app_logger.info(f"[{store_name}] Clicked 'Customised' tab")
        
        # Wait for the date picker widget to load after tab click
        # We use a specific selector that we know should appear
        try:
            await page.wait_for_selector("kat-date-range-picker", state="attached", timeout=5000)
            app_logger.info(f"[{store_name}] 'kat-date-range-picker' attached to DOM")
        except TimeoutError:
            app_logger.warning(f"[{store_name}] 'kat-date-range-picker' did not attach within 5s, trying generic wait")
            await page.wait_for_timeout(2000)
        
        # Step 2: Wait for date picker to appear
        date_picker = await _wait_for_date_picker(page, 10000)
        app_logger.info(f"[{store_name}] Date picker is visible")
        
        # Step 3: Fill in date and time fields
        # Date inputs are type="text" within the date picker
        date_inputs = date_picker.locator('input[type="text"]')
        # Wait for inputs to be ready
        await expect(date_inputs.first).to_be_visible(timeout=5000)
        
        await date_inputs.nth(0).fill(date_range['start_date'])
        await date_inputs.nth(1).fill(date_range['end_date'])
        app_logger.info(f"[{store_name}] Filled date fields: {date_range['start_date']} to {date_range['end_date']}")
        
        # Time dropdowns - look for select elements or kat-dropdown
        time_selects = date_picker.locator('select, kat-dropdown')
        if await time_selects.count() >= 2:
            # Fill start time
            await time_selects.nth(0).click()
            await page.get_by_text(date_range['start_time'], exact=True).click()
            # Fill end time
            await time_selects.nth(1).click()
            await page.get_by_text(date_range['end_time'], exact=True).click()
            app_logger.info(f"[{store_name}] Filled time fields: {date_range['start_time']} to {date_range['end_time']}")
        else:
            # This is common for some views (e.g. Shopper Performance) which only allow date selection
            app_logger.info(f"[{store_name}] Time selectors not found (likely date-only view), proceeding with dates only")
        
        # Step 4: Click "Apply" and wait for metrics response
        apply_btn = page.get_by_role("button", name="Apply")
        async with page.expect_response(
            lambda r: ("summationMetrics" in r.url or "/api/metrics" in r.url) and r.status == 200,
            timeout=30000
        ) as apply_info:
            await apply_btn.click(timeout=action_timeout)
        
        apply_response = await apply_info.value
        app_logger.info(f"[{store_name}] Date/time range applied successfully, received metrics response")
        return True
        
    except AssertionError as e:
        app_logger.warning(f"[{store_name}] Could not apply date range (UI element not found): {e}")
        return False
    except TimeoutError as e:
        app_logger.warning(f"[{store_name}] Timeout while applying date range: {e}")
        return False
    except Exception as e:
        app_logger.error(f"[{store_name}] Unexpected error applying date range: {e}", exc_info=debug_mode)
        return False
