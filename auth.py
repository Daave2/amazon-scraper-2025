# =======================================================================================
#                    AUTH MODULE - Authentication & Session Management
# =======================================================================================

import json
import re
import pyotp
from playwright.async_api import Page, Browser, TimeoutError, expect, Error as PlaywrightError
from typing import Any


async def check_if_login_needed(page: Page, test_url: str, page_timeout: int, debug_mode: bool, app_logger) -> bool:
    app_logger.info(f"Verifying session status by navigating to: {test_url}")
    try:
        # We don't wait for 'load' event to finish because it might take time.
        # We just want to see if we land on login page or dashboard.
        await page.goto(test_url, timeout=page_timeout, wait_until="domcontentloaded")
        
        # Smart wait: Race between Login elements and Dashboard elements
        # If we see login inputs -> Login needed
        # If we see dashboard elements -> Login NOT needed
        
        login_selector = "input#ap_email, input#ap_password, input[name='email']"
        dashboard_selector = "#content > div > div.mainAppContainerExternal"
        
        try:
            # Wait for either to appear
            found = await page.locator(f"{login_selector}, {dashboard_selector}").first.is_visible(timeout=10000)
            if not found:
                # Fallback check on URL if neither appeared quickly
                if "signin" in page.url.lower() or "/ap/" in page.url:
                    return True
                return True # Assume needed if we can't verify dashboard
        except TimeoutError:
             # If timeout, check URL one last time
            if "signin" in page.url.lower() or "/ap/" in page.url:
                return True
            return True

        # If we are here, something is visible. Check what it is.
        if await page.locator(login_selector).first.is_visible():
            app_logger.info("Login form detected.")
            return True
            
        if await page.locator(dashboard_selector).is_visible():
            app_logger.info("Dashboard detected. Session is valid.")
            return False
            
        return True
    except Exception as e:
        app_logger.error(f"Error during session check: {e}", exc_info=debug_mode)
        return True


async def perform_login_and_otp(page: Page, login_url: str, config: dict, page_timeout: int, 
                                debug_mode: bool, app_logger, _save_screenshot_func) -> bool:
    app_logger.info(f"Navigating to login page: {login_url}")
    try:
        await page.goto(login_url, timeout=page_timeout, wait_until="load")
        app_logger.info("Initial page loaded. Determining login flow...")

        continue_shopping_selector = 'button:has-text("Continue shopping")'
        email_field_selector = 'input#ap_email'

        await page.wait_for_selector(f"{continue_shopping_selector}, {email_field_selector}", state="visible", timeout=15000)
        
        if await page.locator(continue_shopping_selector).is_visible():
            app_logger.info("Flow: Interstitial 'Continue shopping' page detected. Clicking it.")
            await page.locator(continue_shopping_selector).click()
            await expect(page.locator(email_field_selector)).to_be_visible(timeout=15000)
        else:
            app_logger.info("Flow: Login form with email field loaded directly.")
        
        email_locator = page.get_by_label("Email or mobile phone number")
        try:
            await email_locator.fill(config['login_email'])
        except TimeoutError:
            app_logger.warning(
                "Email field label not found or not interactable. Falling back to direct selector.")
            fallback_email_field = page.locator(email_field_selector)
            await expect(fallback_email_field).to_be_visible(timeout=10000)
            await fallback_email_field.fill(config['login_email'])

        continue_locator = page.get_by_label("Continue")
        try:
            await continue_locator.click()
        except TimeoutError:
            app_logger.warning(
                "Continue control not available via label. Using fallback selector.")
            fallback_continue = page.get_by_role("button", name=re.compile("continue", re.I))
            if await fallback_continue.count() == 0:
                fallback_continue = page.locator("input#continue, button#continue, input[name='continue']")
            await expect(fallback_continue.first).to_be_visible(timeout=10000)
            await fallback_continue.first.click()

        password_field = page.get_by_label("Password")
        try:
            await expect(password_field).to_be_visible(timeout=10000)
        except TimeoutError:
            app_logger.warning(
                "Password field not visible after entering email. Attempting to bypass passkey flow.")

            async def _click_if_visible(locator: Any) -> bool:
                try:
                    if locator and await locator.count() > 0:
                        visible_locator = locator.first
                        if await visible_locator.is_visible():
                            await visible_locator.click()
                            return True
                except PlaywrightError as inner_error:
                    app_logger.debug(
                        f"Encountered error while handling alternate sign-in option: {inner_error}",
                        exc_info=debug_mode,
                    )
                return False

            bypass_attempted = False

            other_ways_button = page.get_by_role("button", name=re.compile("other ways to sign in", re.I))
            if await _click_if_visible(other_ways_button):
                app_logger.info("Clicked 'Other ways to sign in' button to reveal password option.")
                bypass_attempted = True

            if not bypass_attempted:
                passkey_bypass_selectors = [
                    page.get_by_role("button", name=re.compile("use( your)? password", re.I)),
                    page.get_by_role("link", name=re.compile("use( your)? password", re.I)),
                    page.locator("text=/Use (your )?password/i"),
                    page.locator("text=/Sign-in without passkey/i"),
                ]
                for locator in passkey_bypass_selectors:
                    if await _click_if_visible(locator):
                        app_logger.info("Clicked alternate sign-in option to fall back to password entry.")
                        bypass_attempted = True
                        break

            if not bypass_attempted:
                app_logger.warning(
                    "No passkey bypass option detected. Proceeding without additional interaction.")

            await expect(password_field).to_be_visible(timeout=10000)
        await password_field.fill(config['login_password'])
        await page.get_by_label("Sign in").click()
        
        otp_selector = 'input[id*="otp"]'
        dashboard_selector = "#content > div > div.mainAppContainerExternal"
        await page.wait_for_selector(f"{otp_selector}, {dashboard_selector}", timeout=30000)

        otp_field = page.locator(otp_selector)
        if await otp_field.is_visible():
            app_logger.info("Two-Step Verification (OTP) is required.")
            otp_code = pyotp.TOTP(config['otp_secret_key']).now()
            await otp_field.fill(otp_code)
            if await page.locator("input[type='checkbox'][name='rememberDevice']").is_visible():
                await page.locator("input[type='checkbox'][name='rememberDevice']").check()
            await page.get_by_role("button", name="Sign in").click()

        account_picker_selector = 'h1:has-text("Select an account")'
        await page.wait_for_selector(f"{dashboard_selector}, {account_picker_selector}", timeout=30000)
        
        app_logger.info("Login process appears fully successful.")
        return True
    except Exception as e:
        app_logger.critical(f"Critical error during login process: {e}", exc_info=debug_mode)
        await _save_screenshot_func(page, "login_critical_failure")
        return False


async def prime_master_session(browser: Browser, storage_state_path: str, page_timeout: int, 
                               action_timeout: int, perform_login_func, app_logger) -> bool:
    app_logger.info("Priming master session")
    ctx = None
    try:
        if not browser or not browser.is_connected(): return False
        ctx = await browser.new_context()
        ctx.set_default_navigation_timeout(page_timeout)
        ctx.set_default_timeout(action_timeout)
        await ctx.route("**/*", lambda route: route.abort() if route.request.resource_type in ("image", "stylesheet", "font", "media") else route.continue_())
        page = await ctx.new_page()
        if not await perform_login_func(page): return False
        storage = await ctx.storage_state()
        with open(storage_state_path, 'w') as f: json.dump(storage, f)
        app_logger.info(f"Login successful. Auth state saved to '{storage_state_path}'.")
        return True
    except Exception as e:
        app_logger.exception(f"Priming failed with an unexpected error: {e}")
        return False
    finally:
        if ctx: await ctx.close()
