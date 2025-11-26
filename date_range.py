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
        
        # Define STORE_PREFIX_RE locally as requested
        STORE_PREFIX_RE = re.compile(r"^morrisons\s*-\s*", re.I)
        
        # Take a debug screenshot of the date picker to see what we are dealing with
        await _save_screenshot(page, f"debug_datepicker_{sanitize_store_name(store_name, STORE_PREFIX_RE)}", "output", timezone('Europe/London'), app_logger)
        
        # Step 3: Fill in date and time fields
        # Date inputs are type="text" within the date picker
        date_inputs = date_picker.locator('input[type="text"]')
        # Wait for inputs to be ready
        await expect(date_inputs.first).to_be_visible(timeout=5000)
        
        # Strategy 0: Check for Date Dropdowns (Month, Day, Year selects)
        # The INF page often uses 3 selects per date (Month, Day, Year) -> 6 total
        all_selects = page.locator('select')
        select_count = await all_selects.count()
        
        if select_count >= 6:
            app_logger.info(f"[{store_name}] Found {select_count} dropdowns, attempting to set dates via selects")
            try:
                # Parse dates (MM/DD/YYYY)
                s_m, s_d, s_y = date_range['start_date'].split('/')
                e_m, e_d, e_y = date_range['end_date'].split('/')
                
                # Remove leading zeros if needed (e.g. 01 -> 1)
                # We try both with and without leading zeros to be safe
                
                # Helper to select date part
                async def select_date_part(nth, val):
                    sel = all_selects.nth(nth)
                    # Try exact match first
                    try:
                        await sel.select_option(label=val)
                    except:
                        try:
                            await sel.select_option(value=val)
                        except:
                            # Try stripping leading zero (05 -> 5)
                            if val.startswith('0'):
                                val_no_zero = val[1:]
                                try:
                                    await sel.select_option(label=val_no_zero)
                                except:
                                    await sel.select_option(value=val_no_zero)
                
                # Assume order: Month, Day, Year (based on screenshot: 11, 26, 2025)
                # Start Date
                await select_date_part(0, s_m) # Month
                await select_date_part(1, s_d) # Day
                await select_date_part(2, s_y) # Year
                
                # End Date
                await select_date_part(3, e_m) # Month
                await select_date_part(4, e_d) # Day
                await select_date_part(5, e_y) # Year
                
                app_logger.info(f"[{store_name}] Set dates via dropdowns: {date_range['start_date']} to {date_range['end_date']}")
                
            except Exception as e:
                app_logger.warning(f"[{store_name}] Failed to set dates via dropdowns: {e}")

        # Helper to robustly fill an input
        async def fill_date_input(locator, value):
            await locator.click()
            await locator.clear()
            # Type slowly to ensure events fire
            await locator.type(value, delay=50)
            await locator.press('Enter')
            # Force update via JS just in case
            await locator.evaluate("(el, val) => { el.value = val; el.dispatchEvent(new Event('input', {bubbles: true})); el.dispatchEvent(new Event('change', {bubbles: true})); }", value)

        # Fill date fields
        await fill_date_input(date_inputs.nth(0), date_range['start_date'])
        await fill_date_input(date_inputs.nth(1), date_range['end_date'])
        
        # Trigger change events/blur to ensure UI updates
        await date_inputs.nth(1).blur()
        await page.wait_for_timeout(1000) # Wait for UI to react
        
        app_logger.info(f"[{store_name}] Filled date fields: {date_range['start_date']} to {date_range['end_date']}")
        
        # Time dropdowns - try multiple selector strategies
        time_filled = False
        
        # First, let's see what's actually in the date picker
        app_logger.debug(f"[{store_name}] === Debugging date picker contents ===")
        all_selects = await page.locator('select').count()
        all_dropdowns = await page.locator('kat-dropdown').count()
        all_buttons = await date_picker.locator('button').count()
        app_logger.debug(f"[{store_name}] Page-wide: {all_selects} select elements, {all_dropdowns} kat-dropdown elements")
        app_logger.debug(f"[{store_name}] In date picker: {all_buttons} button elements")
        
        # Strategy 1: Look for select elements
        time_selects = date_picker.locator('select')
        select_count = await time_selects.count()
        app_logger.debug(f"[{store_name}] Found {select_count} select elements in date picker")
        
        if select_count >= 2:
            try:
                # Fill start time
                await time_selects.nth(0).select_option(label=date_range['start_time'])
                # Fill end time  
                await time_selects.nth(1).select_option(label=date_range['end_time'])
                app_logger.info(f"[{store_name}] ✓ Filled time fields via select: {date_range['start_time']} to {date_range['end_time']}")
                time_filled = True
            except Exception as e:
                app_logger.debug(f"[{store_name}] Select option strategy failed: {e}")
        
        # Strategy 2: Look for kat-dropdown elements (clickable dropdowns)
        if not time_filled:
            time_dropdowns = date_picker.locator('kat-dropdown')
            dropdown_count = await time_dropdowns.count()
            app_logger.debug(f"[{store_name}] Found {dropdown_count} kat-dropdown elements in date picker")
            
            if dropdown_count >= 2:
                try:
                    # Fill start time
                    await time_dropdowns.nth(0).click()
                    await page.wait_for_timeout(500)
                    await page.get_by_text(date_range['start_time'], exact=True).click()
                    await page.wait_for_timeout(500)
                    # Fill end time
                    await time_dropdowns.nth(1).click()
                    await page.wait_for_timeout(500)
                    await page.get_by_text(date_range['end_time'], exact=True).click()
                    app_logger.info(f"[{store_name}] ✓ Filled time fields via dropdown: {date_range['start_time']} to {date_range['end_time']}")
                    time_filled = True
                except Exception as e:
                    app_logger.debug(f"[{store_name}] Dropdown click strategy failed: {e}")
        
        # Strategy 3: Look for any time-related buttons or inputs
        if not time_filled:
            # Check for various time-related elements
            time_buttons = await date_picker.locator('button:has-text("AM"), button:has-text("PM"), button:has-text(":")').count()
            time_labels = await date_picker.locator('[aria-label*="time" i], [placeholder*="time" i]').count()
            
            app_logger.debug(f"[{store_name}] Found {time_buttons} time buttons, {time_labels} time labels/placeholders")
            
            if time_buttons > 0 or time_labels > 0:
                app_logger.warning(f"[{store_name}] ⚠ Time inputs detected ({time_buttons} buttons, {time_labels} labels) but could not auto-fill")
        
        # Strategy 4: Check if there are extra text inputs (indices 2 and 3)
        if not time_filled:
            # We already defined date_inputs earlier: date_picker.locator('input[type="text"]')
            text_input_count = await date_inputs.count()
            app_logger.debug(f"[{store_name}] Total text inputs found: {text_input_count}")
            
            if text_input_count >= 4:
                try:
                    # Assume 0,1 are dates (already filled) and 2,3 are times
                    app_logger.info(f"[{store_name}] Found 4+ text inputs, attempting to fill indices 2 and 3 as times")
                    await date_inputs.nth(2).fill(date_range['start_time'])
                    await date_inputs.nth(3).fill(date_range['end_time'])
                    app_logger.info(f"[{store_name}] ✓ Filled time fields via text inputs: {date_range['start_time']} to {date_range['end_time']}")
                    time_filled = True
                except Exception as e:
                    app_logger.debug(f"[{store_name}] Text input strategy failed: {e}")

        # Strategy 5: Look for input[type="time"] or inputs with time-related classes
        if not time_filled:
            explicit_time_inputs = date_picker.locator('input[type="time"], input[class*="time"]')
            explicit_count = await explicit_time_inputs.count()
            app_logger.debug(f"[{store_name}] Found {explicit_count} explicit time inputs")
            
            if explicit_count >= 2:
                try:
                    app_logger.info(f"[{store_name}] Found explicit time inputs, attempting to fill")
                    await explicit_time_inputs.nth(0).fill(date_range['start_time'])
                    await explicit_time_inputs.nth(1).fill(date_range['end_time'])
                    app_logger.info(f"[{store_name}] ✓ Filled time fields via explicit inputs: {date_range['start_time']} to {date_range['end_time']}")
                    time_filled = True
                except Exception as e:
                    app_logger.debug(f"[{store_name}] Explicit time input strategy failed: {e}")

        # Strategy 6: JS Injection (Nuclear Option)
        if not time_filled:
            # Try to find inputs by value or placeholder and set them via JS
            # This handles cases where inputs are hidden or have obscure selectors
            try:
                js_success = await page.evaluate("""({start, end}) => {
                    const inputs = Array.from(document.querySelectorAll('input, select'));
                    
                    // Helper to check if element is time-related
                    const isTimeInput = (el) => {
                        const str = (el.className + ' ' + el.placeholder + ' ' + el.ariaLabel + ' ' + el.name).toLowerCase();
                        return el.type === 'time' || str.includes('time') || str.includes('hour') || str.includes('minute');
                    };
                    
                    const timeInputs = inputs.filter(isTimeInput);
                    
                    if (timeInputs.length >= 2) {
                        // Set start time
                        timeInputs[0].value = start;
                        timeInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
                        timeInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
                        
                        // Set end time
                        timeInputs[1].value = end;
                        timeInputs[1].dispatchEvent(new Event('input', { bubbles: true }));
                        timeInputs[1].dispatchEvent(new Event('change', { bubbles: true }));
                        return true;
                    }
                    return false;
                }""", {'start': date_range['start_time'], 'end': date_range['end_time']})
                
                if js_success:
                    app_logger.info(f"[{store_name}] ✓ Filled time fields via JS injection")
                    time_filled = True
            except Exception as e:
                app_logger.debug(f"[{store_name}] JS injection strategy failed: {e}")

        if not time_filled:
            # No time selectors found
            app_logger.info(f"[{store_name}] Time selectors not found (date-only view), proceeding with dates only")
        
        
        # Step 4: Click "Apply" or "Submit" and wait for metrics response
        # Main dashboard uses "Apply", INF page uses "Submit"
        
        # Try multiple selectors for the Apply button
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
                app_logger.debug(f"[{store_name}] Found Apply/Submit button using selector: {candidate}")
                break
        
        if apply_btn:
            # Use a broader response filter since INF page might use different endpoints
            async with page.expect_response(
                lambda r: r.status == 200 and ("metrics" in r.url or "inventory" in r.url or "submit" in r.url or "dashboard" in r.url),
                timeout=30000
            ) as apply_info:
                await apply_btn.click(timeout=action_timeout)
            
            try:
                await apply_info.value
                app_logger.info(f"[{store_name}] Date/time range applied successfully (clicked button)")
            except Exception:
                app_logger.warning(f"[{store_name}] Clicked apply/submit, but didn't catch expected network response (might still have worked)")
            
            # Verify date picker is gone
            try:
                await expect(date_picker).to_be_hidden(timeout=3000)
                app_logger.info(f"[{store_name}] Date picker closed successfully")
            except:
                app_logger.warning(f"[{store_name}] Date picker might still be open - click may have failed")
                
            return True
        else:
            app_logger.warning(f"[{store_name}] Could not find 'Apply' or 'Submit' button to confirm date range")
            # Take a screenshot to see why we missed the button
            await _save_screenshot(page, f"debug_missing_apply_{sanitize_store_name(store_name, STORE_PREFIX_RE)}", "output", timezone('Europe/London'), app_logger)
            return False
        
    except AssertionError as e:
        app_logger.warning(f"[{store_name}] Could not apply date range (UI element not found): {e}")
        return False
    except TimeoutError as e:
        app_logger.warning(f"[{store_name}] Timeout while applying date range: {e}")
        return False
    except Exception as e:
        app_logger.error(f"[{store_name}] Unexpected error applying date range: {e}", exc_info=debug_mode)
        return False
