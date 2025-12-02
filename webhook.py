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
import urllib.parse

# Google Chat Colors (Used for Performance Highlights)
COLOR_RED = "#C62828"   # Dark Red

def _format_metric_with_emoji(value_str: str, threshold: float, emoji_green: str, 
                              emoji_red: str, is_uph: bool = False) -> str:
    """
    Applies a pass/fail emoji.
    COMPACT MODE: Removes spaces and '%' symbols to save width on mobile.
    """
    try:
        # Clean string to just numbers and decimal point
        clean_str = re.sub(r'[^\d.]', '', value_str)
        if not clean_str:
            return value_str.replace(" ", "") # Return compacted string
            
        numeric_value = float(clean_str)
        is_good = (numeric_value >= threshold) if is_uph else (numeric_value <= threshold)
        emoji = emoji_green if is_good else emoji_red
        
        # Mobile Optimization: No space between emoji and number, no % sign
        return f"{emoji}{clean_str}"
    except (ValueError, TypeError):
        return value_str


async def post_to_chat_webhook(entries: List[Dict[str, str]], chat_webhook_url: str,
                               chat_batch_count: int, get_date_range_func, sanitize_func,
                               uph_threshold: float, lates_threshold: float, inf_threshold: float,
                               emoji_green: str, emoji_red: str, local_timezone, debug_mode: bool, app_logger):
    """
    Send a report using a MOBILE-OPTIMIZED GRID layout.
    - No Borders (Saves padding)
    - Short Headers
    - Compact Metrics
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

        # --- COMPACT GRID LAYOUT ---
        # Shortened titles for Mobile readability
        grid_items = [
            {"title": "Store", "textAlignment": "START"},
            {"title": "Ord", "textAlignment": "CENTER"},   # Was "Orders"
            {"title": "UPH", "textAlignment": "CENTER"},
            {"title": "Lat%", "textAlignment": "CENTER"},  # Was "Lates", added % to header
            {"title": "INF%", "textAlignment": "CENTER"},  # Was "INF", added % to header
        ]

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

            # Apply emoji formatting (Compacted)
            formatted_uph = _format_metric_with_emoji(uph_val, uph_threshold, emoji_green, emoji_red, is_uph=True)
            formatted_lates = _format_metric_with_emoji(lates_val, lates_threshold, emoji_green, emoji_red)
            formatted_inf = _format_metric_with_emoji(inf_val, inf_threshold, emoji_green, emoji_red)

            # Store Name: Truncate nicely if too long for mobile column
            store_name = sanitize_func(entry.get("store", "N/A"))
            # Optional: aggressive truncation for very long names if needed
            # if len(store_name) > 15: store_name = store_name[:14] + "…"

            grid_items.extend([
                {"title": store_name, "textAlignment": "START"},
                {"title": orders_val, "textAlignment": "CENTER"},
                {"title": formatted_uph, "textAlignment": "CENTER"},
                {"title": formatted_lates, "textAlignment": "CENTER"},
                {"title": formatted_inf, "textAlignment": "CENTER"},
            ])
        
        table_section = {
            "header": "Key Performance Indicators",
            "widgets": [{
                "grid": {
                    "title": "Performance Summary",
                    "columnCount": 5, 
                    # NO_BORDER saves significant width padding on mobile
                    "borderStyle": {"type": "NO_BORDER"}, 
                    "items": grid_items
                }
            }]
        }

        payload = {
            "cardsV2": [{
                "cardId": f"batch-summary-{chat_batch_count}",
                "card": {
                    "header": {
                        "title": "Seller Central Metrics Report",
                        "subtitle": card_subtitle,
                        "imageUrl": "https://static.vecteezy.com/system/resources/previews/006/724/659/non_2x/bar-chart-logo-icon-sign-symbol-design-vector.jpg",
                        "imageType": "CIRCLE"
                    },
                    "sections": [table_section],
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
                    app_logger.error(f"Chat webhook post failed: {resp.status} {error_text}")

    except Exception as e:
        app_logger.error(f"Error posting to chat webhook: {e}", exc_info=debug_mode)


async def post_job_summary(total: int, success: int, failures: List[str], duration: float,
                           chat_webhook_url: str, metrics_lock, metrics: dict, 
                           local_timezone, debug_mode: bool, app_logger, apps_script_url: str = None):
    """Send a job summary with ONE main collapse and Quick Actions buttons."""
    if not chat_webhook_url: return
    try:
        status_text = "✅ Job Completed Successfully"
        if failures:
            status_text = f"⚠️ Job Completed with {len(failures)} Failures"
        
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
        avg_sub = sum(t[1] for t in sub_times) / len(sub_times) if sub_times else 0
        sorted_coll = sorted([t[1] for t in coll_times])
        p95_coll = sorted_coll[int(len(sorted_coll) * 0.95)] if sorted_coll else 0
        fastest_store = min(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        slowest_store = max(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        
        bottleneck_msg = "Balanced Flow"
        if avg_coll > 2.0: bottleneck_msg = "🐢 Slow Scraping (Browser Lag)"
        elif avg_sub > 1.0: bottleneck_msg = "🐢 Slow Submission (Webhook Lag)"

        # --- Section 1: High Level ---
        high_level_widgets = [
            {"decoratedText": {"topLabel": "Throughput", "text": f"{throughput_spm:.1f} stores/min", "startIcon": {"knownIcon": "FLIGHT_DEPARTURE"}}},
            {"decoratedText": {"topLabel": "Success Rate", "text": f"{success}/{total} ({success_rate:.1f}%)", "startIcon": {"knownIcon": "STAR"}}},
            {"decoratedText": {"topLabel": "Total Duration", "text": f"{duration:.2f}s", "startIcon": {"knownIcon": "CLOCK"}}}
        ]

        # --- Section 2: Detailed Stats (Grouped) ---
        detailed_widgets = []
        
        # Volume
        detailed_widgets.append({"textParagraph": {"text": "<b>Business Volume 📦</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Total Orders", "text": f"{total_orders:,}", "startIcon": {"knownIcon": "SHOPPING_CART"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Total Units", "text": f"{total_units:,}", "startIcon": {"knownIcon": "TICKET"}}})
        detailed_widgets.append({"divider": {}})

        # Health
        detailed_widgets.append({"textParagraph": {"text": "<b>Resilience & Health 🏥</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Total Retries", "text": str(retries), "startIcon": {"knownIcon": "MEMBERSHIP"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Stores Retried", "text": str(retry_stores), "startIcon": {"knownIcon": "STORE"}}})
        detailed_widgets.append({"divider": {}})

        # Extremes
        detailed_widgets.append({"textParagraph": {"text": "<b>Extremes 📉📈</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Fastest Store", "text": f"{fastest_store[0]} ({fastest_store[1]:.2f}s)", "startIcon": {"knownIcon": "BOLT"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Slowest Store", "text": f"{slowest_store[0]} ({slowest_store[1]:.2f}s)", "startIcon": {"knownIcon": "SNAIL"}}})

        if failures:
            detailed_widgets.append({"divider": {}})
            detailed_widgets.append({"textParagraph": {"text": "<b>Failure Analysis ⚠️</b>"}})
            failure_list = "\n".join([f"• {f}" for f in failures[:5]])
            if len(failures) > 5: failure_list += f"\n...and {len(failures) - 5} more"
            detailed_widgets.append({"textParagraph": {"text": f'<font color="#FF0000">{failure_list}</font>'}})

        # Build sections list
        sections = [
            {"widgets": high_level_widgets},
            {"header": "Detailed Metrics", "collapsible": True, "uncollapsibleWidgetsCount": 0, "widgets": detailed_widgets}
        ]

        payload = {
            "cardsV2": [{
                "cardId": f"job-summary-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": status_text,
                        "subtitle": datetime.now(local_timezone).strftime("%A %d %B, %H:%M"),
                        "imageUrl": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRTGVrSjsDJmQGLCuVWs2Z1fOj1pTcx0ELhBA&s",
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


async def post_quick_actions_card(chat_webhook_url: str, apps_script_url: str, debug_mode: bool, app_logger):
    """Send the Quick Actions card separately so it always posts last."""
    if not chat_webhook_url or not apps_script_url:
        return

    try:
        def build_trigger_url(event_type, date_mode, top_n=None):
            params = {'event_type': event_type, 'date_mode': date_mode}
            if top_n:
                params['top_n'] = top_n
            return f"{apps_script_url}?{urllib.parse.urlencode(params)}"

        quick_actions_widgets = [
            {"textParagraph": {"text": "<b>🚀 Trigger On-Demand Reports</b>"}},
            
            # INF Analysis Today (Top 10)
            {"textParagraph": {"text": "<b>🔍 INF Analysis (Today)</b><br>Top 10 'Item Not Found' items per store for today."}},
            {
                "buttonList": {
                    "buttons": [
                        {
                            "text": "Run INF Analysis",
                            "onClick": {"openLink": {"url": build_trigger_url("run-inf-analysis", "today", "10")}}
                        }
                    ]
                }
            },
            
            # Performance Check
            {"textParagraph": {"text": "<b>📊 Performance Check</b><br>Overview of 'Lates', 'UPH', and key metrics for the current shift."}},
            {
                "buttonList": {
                    "buttons": [
                        {
                            "text": "Run Performance Check",
                            "onClick": {"openLink": {"url": build_trigger_url("run-performance-check", "today")}}
                        }
                    ]
                }
            },
            
            # Yesterday's Report
            {"textParagraph": {"text": "<b>📅 Yesterday's INF Report</b><br>Top 10 INF items per store from yesterday's data."}},
            {
                "buttonList": {
                    "buttons": [
                        {
                            "text": "Run Yesterday's Report",
                            "onClick": {"openLink": {"url": build_trigger_url("run-inf-analysis", "yesterday", "10")}}
                        }
                    ]
                }
            },
            
            # Week-to-Date Report
            {"textParagraph": {"text": "<b>📊 Week-to-Date INF</b><br>Summary of INF from Monday through today."}},
            {
                "buttonList": {
                    "buttons": [
                        {
                            "text": "Run Week-to-Date",
                            "onClick": {"openLink": {"url": build_trigger_url("run-inf-analysis", "week_to_date", "10")}}
                        }
                    ]
                }
            }
        ]

        payload = {
            "cardsV2": [{
                "cardId": f"quick-actions-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": "⚡ Quick Actions",
                        "subtitle": "Run additional reports and checks",
                        "imageUrl": "https://static.vecteezy.com/system/resources/previews/006/724/659/non_2x/bar-chart-logo-icon-sign-symbol-design-vector.jpg",
                        "imageType": "CIRCLE"
                    },
                    "sections": [
                        {"widgets": quick_actions_widgets}
                    ],
                },
            }]
        }

        max_attempts = 3
        backoff_seconds = [1, 2, 4]

        async with aiohttp.ClientSession() as session:
            for attempt in range(1, max_attempts + 1):
                try:
                    async with session.post(chat_webhook_url, json=payload) as response:
                        if response.status < 300:
                            return

                        response_body = await response.text()
                        app_logger.warning(
                            f"Quick actions card POST returned status {response.status} (attempt {attempt}/{max_attempts}). "
                            f"Response: {response_body}"
                        )
                except Exception as request_error:
                    app_logger.error(
                        f"Error posting quick actions card (attempt {attempt}/{max_attempts}): {request_error}",
                        exc_info=debug_mode,
                    )

                if attempt < max_attempts:
                    sleep_seconds = backoff_seconds[attempt - 1]
                    app_logger.info(
                        f"Retrying quick actions card post in {sleep_seconds} seconds (attempt {attempt + 1}/{max_attempts})"
                    )
                    await asyncio.sleep(sleep_seconds)

            raise RuntimeError("Failed to post quick actions card after retrying")

    except Exception as e:
        app_logger.error(f"Error posting quick actions card: {e}", exc_info=debug_mode)


async def post_performance_highlights(store_data: List[Dict[str, str]], chat_webhook_url: str,
                                      sanitize_func, local_timezone, debug_mode: bool, app_logger, apps_script_url: str = None):
    """
    Send highlights using COLUMNS layout to support HTML COLORING.
    """
    app_logger.info(f"post_performance_highlights called with {len(store_data) if store_data else 0} stores")
    
    if not chat_webhook_url:
        app_logger.warning("No chat webhook URL provided to post_performance_highlights")
        return
    if not store_data:
        app_logger.warning("No store data provided to post_performance_highlights")
        return
    
    try:
        parsed_stores = []
        for entry in store_data:
            try:
                if int(float(entry.get('orders', '0'))) == 0: continue
                
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
        
        app_logger.info(f"Parsed {len(parsed_stores)} stores with data (after filtering 0 orders)")
        
        if not parsed_stores:
            app_logger.warning("No stores with non-zero orders to report")
            return
        
        sorted_by_lates = sorted(parsed_stores, key=lambda x: x['lates'], reverse=True)[:5]
        # sorted_by_inf removed as it is now handled by the detailed INF scraper report
        sorted_by_uph = sorted(parsed_stores, key=lambda x: x['uph'])[:5]
        
        sections = []

        # Helper to build the HTML Colored Column widgets
        def build_colored_widgets(title, stores, metric_str_key):
            widgets = []
            for store in stores:
                widgets.append({
                    "columns": {
                        "columnItems": [
                            {
                                "horizontalSizeStyle": "FILL_AVAILABLE_SPACE", 
                                "horizontalAlignment": "START",
                                "widgets": [{"textParagraph": {"text": sanitize_func(store['store'])}}]
                            },
                            {
                                "horizontalSizeStyle": "FILL_AVAILABLE_SPACE", 
                                "horizontalAlignment": "END", 
                                "widgets": [{"textParagraph": {"text": f'<font color="{COLOR_RED}"><b>{store[metric_str_key]}</b></font>'}}]
                            }
                        ]
                    }
                })
            return {"header": title, "widgets": widgets}

        if sorted_by_lates and sorted_by_lates[0]['lates'] > 0:
            app_logger.info(f"Adding Lates section (top: {sorted_by_lates[0]['lates']}%)")
            sections.append(build_colored_widgets("⚠️ Highest Lates %", sorted_by_lates, 'lates_str'))
        
        if sorted_by_uph:
            app_logger.info(f"Adding UPH section (lowest: {sorted_by_uph[0]['uph']})")
            sections.append(build_colored_widgets("⚠️ Lowest UPH", sorted_by_uph, 'uph_str'))

        app_logger.info(f"Performance highlights sections to send: {len(sections)}")
        
        if sections:
            # Add Quick Action button if Apps Script URL is available
            if apps_script_url:
                # Helper to build URL (redefined here as it's a separate function scope)
                params = {'event_type': 'run-performance-check', 'date_mode': 'today'}
                trigger_url = f"{apps_script_url}?{urllib.parse.urlencode(params)}"

                sections.append({
                    "widgets": [
                        {
                            "buttonList": {
                                "buttons": [{
                                    "text": "🔄 Re-run Performance Check",
                                    "onClick": {
                                        "openLink": {
                                            "url": trigger_url
                                        }
                                    }
                                }]
                            }
                        }
                    ]
                })

            import ssl
            import certifi
            
            payload = {
                "cardsV2": [{
                    "cardId": f"perf-high-{int(datetime.now().timestamp())}",
                    "card": {
                        "header": {
                            "title": "📊 Performance Highlights",
                            "subtitle": "Stores requiring attention",
                            "imageUrl": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTDtOSmbsPT97vo-A25rpgqFT5b6_xHxfuw4g&s",
                            "imageType": "CIRCLE"
                        },
                        "sections": sections,
                    },
                }]
            }
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                resp = await session.post(chat_webhook_url, json=payload)
                if resp.status == 200:
                    app_logger.info("Performance highlights sent successfully")
                else:
                    app_logger.error(f"Failed to send performance highlights: {resp.status}")
        else:
            app_logger.warning("No sections to send in performance highlights")

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