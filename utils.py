# =======================================================================================
#                           UTILS MODULE - Logging & Utilities
# =======================================================================================

import logging
import os
import csv
import json
import re
from datetime import datetime
from pytz import timezone
from logging.handlers import RotatingFileHandler
from playwright.async_api import Page
from typing import List, Dict

# Use UK timezone for log timestamps
LOCAL_TIMEZONE = timezone('Europe/London')


class LocalTimeFormatter(logging.Formatter):
    """Formatter that converts timestamps to ``LOCAL_TIMEZONE``."""

    def converter(self, ts: float):
        dt = datetime.fromtimestamp(ts, LOCAL_TIMEZONE)
        return dt.timetuple()


def setup_logging():
    """Configure application logging to file and console.

    Returns:
        Logger: Configured logger instance used throughout the app.
    """
    app_logger = logging.getLogger('app')
    
    # Clear existing handlers to prevent duplicates
    if app_logger.handlers:
        app_logger.handlers.clear()
    
    app_logger.setLevel(logging.INFO)
    app_logger.propagate = False  # Prevent logs from propagating to root logger

    app_file = RotatingFileHandler('app.log', maxBytes=10**7, backupCount=5)
    fmt = LocalTimeFormatter('%(asctime)s %(levelname)s %(message)s')
    app_file.setFormatter(fmt)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    app_logger.addHandler(app_file)
    app_logger.addHandler(console)
    return app_logger


def sanitize_store_name(name: str, store_prefix_re) -> str:
    """Trim standard prefix from store names for chat display."""
    return store_prefix_re.sub("", name).strip()


def sanitize_csv_value(value):
    """Ensure CSV values do not introduce malformed rows.

    Replaces any newline or carriage return characters with spaces and
    trims surrounding whitespace. ``None`` values are converted to an
    empty string, while numeric types are returned as-is to preserve
    their type in the CSV output.
    """

    if value is None:
        return ""

    if isinstance(value, (int, float)):
        return value

    text_value = str(value)
    text_value = text_value.replace("\r", " ").replace("\n", " ")
    return text_value.strip()


async def _save_screenshot(page: Page | None, prefix: str, output_dir: str, local_timezone, app_logger):
    if not page or page.is_closed():
        app_logger.warning(f"Cannot save screenshot '{prefix}': Page is closed or unavailable.")
        return
    try:
        safe_prefix = re.sub(r'[\\/*?:"\u003c\u003e|]', "_", prefix)
        timestamp = datetime.now(local_timezone).strftime("%Y%m%d_%H%M%S")
        path = os.path.join(output_dir, f"{safe_prefix}_{timestamp}.png")
        await page.screenshot(path=path, full_page=True, timeout=15000)
        app_logger.info(f"Screenshot saved for debugging: {path}")
    except Exception as e:
        app_logger.error(f"Failed to save screenshot with prefix '{prefix}': {e}")


def load_default_data(urls_data: List[Dict], app_logger):
    """Load store data from urls.csv."""
    urls_data.clear()
    try:
        with open('urls.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            for i, row in enumerate(reader):
                if not row or len(row) < 4:
                    app_logger.warning(f"Skipping malformed row {i+2} in urls.csv: {row}")
                    continue
                # CSV columns: store_number, merchant_id, new_id, store_name, marketplace_id
                urls_data.append({
                    'store_number': row[0].strip() if len(row) > 0 else '',
                    'merchant_id': row[1].strip() if len(row) > 1 else '',
                    'store_name': row[3].strip() if len(row) > 3 else '',
                    'marketplace_id': row[4].strip() if len(row) > 4 else ''
                })
        app_logger.info(f"{len(urls_data)} stores loaded from urls.csv")
    except FileNotFoundError:
        app_logger.error("FATAL: 'urls.csv' not found. Please ensure the file exists and is named correctly (all lowercase).")
        raise
    except Exception:
        app_logger.exception("An error occurred while loading urls.csv")


def ensure_storage_state(storage_state_path: str, app_logger):
    """Check if storage state file exists and is valid."""
    if not os.path.exists(storage_state_path) or os.path.getsize(storage_state_path) == 0:
        return False
    try:
        with open(storage_state_path) as f:
            data = json.load(f)
        if (
            not isinstance(data, dict)
            or "cookies" not in data
            or not isinstance(data["cookies"], list)
            or not data["cookies"]
        ):
            app_logger.warning("Storage state file exists but is malformed or empty. Forcing re-login.")
            return False
        return True
    except json.JSONDecodeError:
        app_logger.warning("Storage state file is corrupted. Forcing re-login.")
        return False
