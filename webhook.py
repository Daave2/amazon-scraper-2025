# =======================================================================================
#                    WEBHOOK MODULE - Google Chat Webhook Integration
# =======================================================================================

import re
import json
import asyncio
import aiohttp
import ssl
import certifi
import aiofiles
import os
import csv
import io
from datetime import datetime
from typing import List, Dict

# Google Chat Colors
COLOR_GREEN = "#2E7D32" # Dark Green
COLOR_RED = "#C62828"   # Dark Red
COLOR_NEUTRAL = "#5F6368" # Grey

def _format_metric_html(value_str: str, threshold: float, is_uph: bool = False) -> str:
    """Formats a metric string with HTML color tags based on threshold."""
    try:
        clean_str = re.sub(r'[^\d.]', '', value_str)
        if not clean_str:
            return value_str
            
        numeric_value = float(clean_str)
        is_good = (numeric_value >= threshold) if is_uph else (numeric_value <= threshold)
        
        color = COLOR_GREEN if is_good else COLOR_RED
        # Bold the value and color it
        return f'<font color="{color}"><b>{value_str}</b></font>'
    except (ValueError, TypeError):
        return value_str

async def post_to_chat_webhook(entries: List[Dict[str, str]], chat_webhook_url: str,
                               chat_batch_count: int, get_date_range_func, sanitize_func,
                               uph_threshold: float, lates_threshold: float, inf_threshold: float,
                               emoji_green: str, emoji_red: str, local_timezone, debug_mode: bool, app_logger):
    """
    Send a report using Columns layout to support HTML colored text.
    Note: Ensure your chat_batch_size is <= 25, as cards have a 100-widget limit.
    """
    if not chat_webhook_url or not entries:
        return
    try:
        batch_header_text = datetime.now(local_timezone).strftime("%A %d %B, %H:%M")
        card_subtitle = f"{batch_header_text}  Batch {chat_batch_count} ({len(entries)} stores)"
        
        date_range = get_date_range_func()
        if date_range:
            card_subtitle += f" • 📅 {date_range['start_date']} - {date_range['end_date']}"

        # Filter out stores with 0 orders
        filtered_entries = []
        for e in entries:
            try:
                val = e.get('orders', '0')
                if int(float(val)) > 0:
                    filtered_entries.append(e)
            except (ValueError, TypeError):
                continue

        if not filtered_entries:
            return

        sorted_entries = sorted(filtered_entries, key=lambda e: sanitize_func(e.get("store", "")))
        
        # --- Build Widgets using Columns for HTML support ---
        # Card limit is 100 widgets. Each store uses 1 widget (Columns).
        # We also have headers/dividers.
        
        widgets = []
        
        # Legend / Header Row
        widgets.append({
            "columns": {
                "columnItems": [
                    {
                        "horizontalSizeStyle": "FILL_AVAILABLE_SPACE",
                        "widgets": [{"textParagraph": {"text": "<b><font color=\"#5F6368\">Store Name</font></b>"}}]
                    },
                    {
                        "horizontalSizeStyle": "FILL_AVAILABLE_SPACE",
                        "widgets": [{"textParagraph": {"text": "<b><font color=\"#5F6368\">Metrics (Orders | UPH | Lates | INF)</font></b>", "textAlignment": "END"}}]
                    }
                ]
            }
        })
        widgets.append({"divider": {}})

        for entry in sorted_entries:
            # Clean up orders
            orders_raw = entry.get("orders", "0")
            try:
                orders_val = str(int(float(orders_raw)))
            except:
                orders_val = orders_raw

            uph_val = entry.get("uph", "N/A")
            lates_val = entry.get("lates", "0.0 %") or "0.0 %"
            inf_val = entry.get("inf", "0.0 %") or "0.0 %"

            # Apply HTML Color formatting
            fmt_uph = _format_metric_html(uph_val, uph_threshold, is_uph=True)
            fmt_lates = _format_metric_html(lates_val, lates_threshold)
            fmt_inf = _format_metric_html(inf_val, inf_threshold)

            # Construct the metric string
            # Using non-breaking spaces (&nbsp;) for alignment visual
            metric_string = f"{orders_val} &nbsp;|&nbsp; {fmt_uph} &nbsp;|&nbsp; {fmt_lates} &nbsp;|&nbsp; {fmt_inf}"

            widgets.append({
                "columns": {
                    "columnItems": [
                        {
                            # Left Column: Store Name
                            "horizontalSizeStyle": "FILL_AVAILABLE_SPACE",
                            "widgets": [{"textParagraph": {"text": f"<b>{sanitize_func(entry.get('store', 'N/A'))}</b>"}}]
                        },
                        {
                            # Right Column: Metrics
                            "horizontalSizeStyle": "FILL_AVAILABLE_SPACE",
                            "widgets": [{"textParagraph": {"text": metric_string, "textAlignment": "END"}}]
                        }
                    ]
                }
            })
            # Optional: Add a subtle divider between rows if list is long, 
            # but it consumes widget count. Skipping for density.

        # --- Assemble the final payload ---
        payload = {
            "cardsV2": [{
                "cardId": f"batch-summary-{chat_batch_count}",
                "card": {
                    "header": {
                        "title": "Seller Central Metrics Report",
                        "subtitle": card_subtitle,
                        "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": [{"widgets": widgets}],
                },
            }]
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(chat_webhook_url, json=payload) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    app_logger.error(f"Chat webhook post failed. Status: {resp.status}. Response: {error_text}")
    except Exception as e:
        app_logger.error(f"Error posting to chat webhook: {e}", exc_info=debug_mode)


async def post_job_summary(total: int, success: int, failures: List[str], duration: float,
                           chat_webhook_url: str, metrics_lock, metrics: dict, 
                           local_timezone, debug_mode: bool, app_logger):
    """Send a detailed job summary card to Google Chat using Material Icons."""
    if not chat_webhook_url: return
    try:
        status_text = "Job Completed Successfully"
        status_icon = "check_circle" # Material Icon name
        if failures:
            status_text = f"Job Completed with {len(failures)} Failures"
            status_icon = "warning"
        
        success_rate = (success / total) * 100 if total > 0 else 0
        throughput_spm = (success / (duration / 60)) if duration > 0 else 0
        
        async with metrics_lock:
            coll_times = metrics["collection_times"]
            sub_times = metrics["submission_times"]
            retries = metrics["retries"]
            retry_stores = len(metrics["retry_stores"])
            total_orders = metrics["total_orders"]
            total_units = metrics["total_units"]
            
        avg_coll = sum(t[1] for t in coll_times) / len(coll_times) if coll_times else 0
        p95_coll = sorted([t[1] for t in coll_times])[int(len(coll_times) * 0.95)] if coll_times else 0
        fastest_store = min(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        slowest_store = max(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)

        # Bottleneck Analysis logic
        bottleneck_msg = "Balanced Flow"
        if avg_coll > 2.0: bottleneck_msg = "🐢 Slow Scraping (Browser Lag)"
        
        sections = [
            {
                "header": "High-Level Stats",
                "widgets": [
                    {"decoratedText": {"topLabel": "Throughput", "text": f"{throughput_spm:.1f} stores/min", "startIcon": {"materialIcon": {"name": "speed"}}}},
                    {"decoratedText": {"topLabel": "Success Rate", "text": f"{success}/{total} ({success_rate:.1f}%)", "startIcon": {"materialIcon": {"name": "verified"}}}},
                    {"decoratedText": {"topLabel": "Total Duration", "text": f"{duration:.2f}s", "startIcon": {"materialIcon": {"name": "timer"}}}}
                ]
            },
            {
                "header": "Business Volume",
                "collapsible": True,
                "uncollapsibleWidgetsCount": 0,
                "widgets": [
                    {"decoratedText": {"topLabel": "Total Orders", "text": f"{total_orders:,}", "startIcon": {"materialIcon": {"name": "shopping_cart"}}}},
                    {"decoratedText": {"topLabel": "Total Units", "text": f"{total_units:,}", "startIcon": {"materialIcon": {"name": "inventory_2"}}}}
                ]
            },
            {
                "header": "Resilience & Health",
                "collapsible": True,
                "uncollapsibleWidgetsCount": 0,
                "widgets": [
                    {"decoratedText": {"topLabel": "Total Retries", "text": str(retries), "startIcon": {"materialIcon": {"name": "replay"}}}},
                    {"decoratedText": {"topLabel": "Stores Retried", "text": str(retry_stores), "startIcon": {"materialIcon": {"name": "store"}}}}
                ]
            },
            {
                "header": "Speed Breakdown",
                "collapsible": True,
                "uncollapsibleWidgetsCount": 0,
                "widgets": [
                    {"decoratedText": {"topLabel": "Avg Collection Time", "text": f"{avg_coll:.2f}s", "startIcon": {"materialIcon": {"name": "hourglass_empty"}}}},
                    {"decoratedText": {"topLabel": "p95 Collection Time", "text": f"{p95_coll:.2f}s", "startIcon": {"materialIcon": {"name": "hourglass_full"}}}},
                    {"decoratedText": {"topLabel": "Bottleneck Status", "text": bottleneck_msg, "startIcon": {"materialIcon": {"name": "traffic"}}}}
                ]
            },
            {
                "header": "Extremes",
                "collapsible": True,
                "uncollapsibleWidgetsCount": 0,
                "widgets": [
                    {"decoratedText": {"topLabel": "Fastest Store", "text": f"{fastest_store[0]} ({fastest_store[1]:.2f}s)", "startIcon": {"materialIcon": {"name": "bolt"}}}},
                    {"decoratedText": {"topLabel": "Slowest Store", "text": f"{slowest_store[0]} ({slowest_store[1]:.2f}s)", "startIcon": {"materialIcon": {"name": "snooze"}}}}
                ]
            }
        ]
        
        if failures:
            failure_list = "\n".join([f"• {f}" for f in failures[:5]])
            if len(failures) > 5: failure_list += f"\n...and {len(failures) - 5} more"
            sections.append({
                "header": "Failure Analysis",
                "collapsible": True,
                "uncollapsibleWidgetsCount": 0,
                "widgets": [
                    {"textParagraph": {"text": f"<font color=\"#B00020\"><b>Recent Failures:</b>\n{failure_list}</font>"}}
                ]
            })

        payload = {
            "cardsV2": [{
                "cardId": f"job-summary-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": f"{status_text}",
                        "subtitle": datetime.now(local_timezone).strftime("%A %d %B, %H:%M"),
                        "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": sections,
                },
            }]
        }
        
        async with aiohttp.ClientSession() as session:
            await session.post(chat_webhook_url, json=payload)

    except Exception as e:
        app_logger.error(f"Error posting job summary: {e}", exc_info=debug_mode)


async def post_performance_highlights(store_data: List[Dict[str, str]], chat_webhook_url: str,
                                      sanitize_func, local_timezone, debug_mode: bool, app_logger):
    """Send performance highlight cards using Colored Text and Columns."""
    if not chat_webhook_url or not store_data: return
    
    try:
        parsed_stores = []
        for entry in store_data:
            try:
                # Filter 0 orders
                if int(float(entry.get('orders', '0'))) == 0: continue
                
                # Helper for safe float
                def parse_metric(key):
                    clean = re.sub(r'[^0-9.]', '', entry.get(key, '0'))
                    return float(clean) if clean else 0.0

                parsed_stores.append({
                    'store': entry.get('store', 'Unknown'),
                    'lates': parse_metric('lates'), 'lates_str': entry.get('lates', '0%'),
                    'inf': parse_metric('inf'), 'inf_str': entry.get('inf', '0%'),
                    'uph': parse_metric('uph'), 'uph_str': entry.get('uph', '0')
                })
            except: continue
        
        if not parsed_stores: return
        
        # Sorts
        top_lates = sorted(parsed_stores, key=lambda x: x['lates'], reverse=True)[:5]
        top_inf = sorted(parsed_stores, key=lambda x: x['inf'], reverse=True)[:5]
        low_uph = sorted(parsed_stores, key=lambda x: x['uph'])[:5]
        
        sections = []

        def build_highlight_widgets(title, stores, metric_key, metric_str_key, is_reverse=True):
            widgets = []
            for store in stores:
                # Color logic for highlights (Bad is Red)
                val = store[metric_key]
                # For Lates/INF: Higher is Bad (Red). For UPH: Lower is Bad (Red).
                # Since these are "Bottom Performers", we default to Red for emphasis.
                color = COLOR_RED 
                
                widgets.append({
                    "columns": {
                        "columnItems": [
                            {"horizontalSizeStyle": "FILL_AVAILABLE_SPACE", "widgets": [{"textParagraph": {"text": sanitize_func(store['store'])}}]},
                            {"horizontalSizeStyle": "FILL_AVAILABLE_SPACE", "widgets": [{"textParagraph": {"text": f'<font color="{color}"><b>{store[metric_str_key]}</b></font>', "textAlignment": "END"}}]}
                        ]
                    }
                })
            return {"header": title, "widgets": widgets}

        if top_lates and top_lates[0]['lates'] > 0:
            sections.append(build_highlight_widgets("⚠️ Highest Lates %", top_lates, 'lates', 'lates_str'))
        
        if top_inf and top_inf[0]['inf'] > 0:
            sections.append(build_highlight_widgets("⚠️ Highest INF %", top_inf, 'inf', 'inf_str'))
            
        if low_uph:
            # Only show low UPH if it's notably low (e.g., < 100 or just lowest 5)
            sections.append(build_highlight_widgets("⚠️ Lowest UPH", low_uph, 'uph', 'uph_str', is_reverse=False))

        if sections:
            payload = {
                "cardsV2": [{
                    "cardId": f"perf-high-{int(datetime.now().timestamp())}",
                    "card": {
                        "header": {
                            "title": "📊 Performance Highlights",
                            "subtitle": "Stores requiring attention",
                            "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                            "imageType": "CIRCLE"
                        },
                        "sections": sections,
                    },
                }]
            }
            async with aiohttp.ClientSession() as session:
                await session.post(chat_webhook_url, json=payload)

    except Exception as e:
        app_logger.error(f"Error posting highlights: {e}", exc_info=debug_mode)


async def add_to_pending_chat(entry: Dict[str, str], chat_webhook_url: str, pending_chat_lock,
                              pending_chat_entries: List, chat_batch_size: int, post_webhook_func):
    if not chat_webhook_url:
        return
    async with pending_chat_lock:
        pending_chat_entries.append(entry)
        if len(pending_chat_entries) >= chat_batch_size:
            entries_to_send = pending_chat_entries[:chat_batch_size]
            del pending_chat_entries[:chat_batch_size]
            await post_webhook_func(entries_to_send)


async def flush_pending_chat_entries(chat_webhook_url: str, pending_chat_lock,
                                     pending_chat_entries: List, post_webhook_func):
    if not chat_webhook_url:
        return
    async with pending_chat_lock:
        if pending_chat_entries:
            entries = pending_chat_entries[:]
            pending_chat_entries.clear()
            await post_webhook_func(entries)


async def log_submission(data: Dict[str,str], log_lock, log_file: str, json_log_file: str,
                        submitted_data_lock, submitted_store_data: List, 
                        add_to_chat_func, local_timezone, app_logger):
    async with log_lock:
        current_timestamp = datetime.now(local_timezone).strftime('%Y-%m-%d %H:%M:%S')
        log_entry = {'timestamp': current_timestamp, **data}
        fieldnames = ['timestamp','store','orders','units','fulfilled','uph','inf','found','cancelled','lates','time_available']
        new_csv = not os.path.exists(log_file)
        try:
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
            if new_csv:
                writer.writeheader()
            writer.writerow(log_entry)
            async with aiofiles.open(log_file, 'a', newline='', encoding='utf-8') as f:
                await f.write(csv_buffer.getvalue())
        except IOError as e:
            app_logger.error(f"Error writing to CSV log file {log_file}: {e}")
        try:
            async with aiofiles.open(json_log_file, 'a', encoding='utf-8') as f:
                await f.write(json.dumps(log_entry) + '\n')
        except IOError as e:
            app_logger.error(f"Error writing to JSON log file {json_log_file}: {e}")
        
        async with submitted_data_lock:
            submitted_store_data.append(data)
        
        await add_to_chat_func(log_entry)