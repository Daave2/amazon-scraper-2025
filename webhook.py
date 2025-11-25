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

def _format_metric_with_emoji(value_str: str, threshold: float, emoji_green: str, 
                              emoji_red: str, is_uph: bool = False) -> str:
    """Applies a pass/fail emoji to a metric string based on a threshold."""
    try:
        # Clean string to just numbers and decimal point
        clean_str = re.sub(r'[^\d.]', '', value_str)
        if not clean_str:
            return value_str
            
        numeric_value = float(clean_str)
        is_good = (numeric_value >= threshold) if is_uph else (numeric_value <= threshold)
        emoji = emoji_green if is_good else emoji_red
        return f"{emoji} {value_str}"
    except (ValueError, TypeError):
        return value_str # Return as is if not a number


async def post_to_chat_webhook(entries: List[Dict[str, str]], chat_webhook_url: str,
                               chat_batch_count: int, get_date_range_func, sanitize_func,
                               uph_threshold: float, lates_threshold: float, inf_threshold: float,
                               emoji_green: str, emoji_red: str, local_timezone, debug_mode: bool, app_logger):
    """
    Send a report using the GRID layout (cleaner borders) with Emojis.
    Includes the 'Orders' column.
    """
    if not chat_webhook_url or not entries:
        return
    try:
        batch_header_text = datetime.now(local_timezone).strftime("%A %d %B, %H:%M")
        card_subtitle = f"{batch_header_text}  Batch {chat_batch_count} ({len(entries)} stores)"
        
        # Add date range to subtitle if active
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

        # --- Build the Grid/Table Widget ---
        # 5 Columns: Store | Orders | UPH | Lates | INF
        grid_items = [
            {"title": "Store", "textAlignment": "START"},
            {"title": "Orders", "textAlignment": "CENTER"},
            {"title": "UPH", "textAlignment": "CENTER"},
            {"title": "Lates", "textAlignment": "CENTER"},
            {"title": "INF", "textAlignment": "CENTER"},
        ]

        for entry in sorted_entries:
            # Clean up orders (e.g., "20.0" -> "20")
            orders_raw = entry.get("orders", "0")
            try:
                orders_val = str(int(float(orders_raw)))
            except:
                orders_val = orders_raw

            uph_val = entry.get("uph", "N/A")
            lates_val = entry.get("lates", "0.0 %") or "0.0 %"
            inf_val = entry.get("inf", "0.0 %") or "0.0 %"

            # Apply emoji formatting (Grid supports Emojis, not HTML colors)
            formatted_uph = _format_metric_with_emoji(uph_val, uph_threshold, emoji_green, emoji_red, is_uph=True)
            formatted_lates = _format_metric_with_emoji(lates_val, lates_threshold, emoji_green, emoji_red)
            formatted_inf = _format_metric_with_emoji(inf_val, inf_threshold, emoji_green, emoji_red)

            grid_items.extend([
                {"title": sanitize_func(entry.get("store", "N/A")), "textAlignment": "START"},
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
                    "borderStyle": {"type": "STROKE", "cornerRadius": 4},
                    "items": grid_items
                }
            }]
        }

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
                    app_logger.error(
                        f"Chat webhook post failed. Status: {resp.status}. Response: {error_text}"
                    )
    except Exception as e:
        app_logger.error(f"Error posting to chat webhook: {e}", exc_info=debug_mode)


async def post_job_summary(total: int, success: int, failures: List[str], duration: float,
                           chat_webhook_url: str, metrics_lock, metrics: dict, 
                           local_timezone, debug_mode: bool, app_logger):
    """Send a job summary. Uses Unicode Emoji for title and Groups all details into ONE collapse."""
    if not chat_webhook_url: return
    try:
        # Use Unicode Emoji directly in string for the Header Title
        status_text = "✅ Job Completed Successfully"
        if failures:
            status_text = f"⚠️ Job Completed with {len(failures)} Failures"
        
        success_rate = (success / total) * 100 if total > 0 else 0
        throughput_spm = (success / (duration / 60)) if duration > 0 else 0
        
        # Calculate Analytics
        async with metrics_lock:
            coll_times = metrics["collection_times"]
            sub_times = metrics["submission_times"]
            retries = metrics["retries"]
            retry_stores = len(metrics["retry_stores"])
            total_orders = metrics["total_orders"]
            total_units = metrics["total_units"]
            
        avg_coll = sum(t[1] for t in coll_times) / len(coll_times) if coll_times else 0
        sorted_coll = sorted([t[1] for t in coll_times])
        p95_coll = sorted_coll[int(len(sorted_coll) * 0.95)] if sorted_coll else 0
        fastest_store = min(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        slowest_store = max(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        
        bottleneck_msg = "Balanced Flow"
        if avg_coll > 2.0: bottleneck_msg = "🐢 Slow Scraping (Browser Lag)"
        elif avg_sub > 1.0: bottleneck_msg = "🐢 Slow Submission (Webhook Lag)"
        elif avg_coll < 1.0 and avg_sub < 0.5: bottleneck_msg = "🚀 High Speed (No Bottlenecks)"

        # --- Section 1: High Level (Always Visible) ---
        high_level_widgets = [
            {"decoratedText": {"topLabel": "Throughput", "text": f"{throughput_spm:.1f} stores/min", "startIcon": {"knownIcon": "FLIGHT_DEPARTURE"}}},
            {"decoratedText": {"topLabel": "Success Rate", "text": f"{success}/{total} ({success_rate:.1f}%)", "startIcon": {"knownIcon": "STAR"}}},
            {"decoratedText": {"topLabel": "Total Duration", "text": f"{duration:.2f}s", "startIcon": {"knownIcon": "CLOCK"}}}
        ]

        # --- Section 2: Detailed Stats (ALL grouped in one collapse) ---
        # We use textParagraphs as "Sub-headers" followed by the data widgets
        detailed_widgets = []

        # Business Volume
        detailed_widgets.append({"textParagraph": {"text": "<b>Business Volume 📦</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Total Orders", "text": f"{total_orders:,}", "startIcon": {"knownIcon": "SHOPPING_CART"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Total Units", "text": f"{total_units:,}", "startIcon": {"knownIcon": "TICKET"}}})
        detailed_widgets.append({"divider": {}})

        # Resilience
        detailed_widgets.append({"textParagraph": {"text": "<b>Resilience & Health 🏥</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Total Retries", "text": str(retries), "startIcon": {"knownIcon": "MEMBERSHIP"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Stores Retried", "text": str(retry_stores), "startIcon": {"knownIcon": "STORE"}}})
        detailed_widgets.append({"divider": {}})

        # Speed
        detailed_widgets.append({"textParagraph": {"text": "<b>Speed Breakdown ⏱️</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Avg Collection Time", "text": f"{avg_coll:.2f}s (Browser)", "startIcon": {"knownIcon": "DESCRIPTION"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "p95 Collection Time", "text": f"{p95_coll:.2f}s", "startIcon": {"knownIcon": "DESCRIPTION"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Bottleneck Status", "text": bottleneck_msg, "startIcon": {"knownIcon": "TRAFFIC"}}})
        detailed_widgets.append({"divider": {}})

        # Extremes
        detailed_widgets.append({"textParagraph": {"text": "<b>Extremes 📉📈</b>"}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Fastest Store", "text": f"{fastest_store[0]} ({fastest_store[1]:.2f}s)", "startIcon": {"knownIcon": "BOLT"}}})
        detailed_widgets.append({"decoratedText": {"topLabel": "Slowest Store", "text": f"{slowest_store[0]} ({slowest_store[1]:.2f}s)", "startIcon": {"knownIcon": "SNAIL"}}})

        # Failures (only add if existing)
        if failures:
            detailed_widgets.append({"divider": {}})
            detailed_widgets.append({"textParagraph": {"text": "<b>Failure Analysis ⚠️</b>"}})
            
            failure_counts = {}
            for f in failures:
                msg = f
                if '(' in f and ')' in f:
                    msg = f[f.rfind('(')+1 : f.rfind(')')]
                failure_counts[msg] = failure_counts.get(msg, 0) + 1
            
            failure_summary = "\n".join([f"• {k}: {v}" for k, v in failure_counts.items()])
            failure_list = "\n".join([f"• {f}" for f in failures[:5]])
            if len(failures) > 5:
                failure_list += f"\n...and {len(failures) - 5} more"
            
            detailed_widgets.append({"textParagraph": {"text": f"<b>Breakdown:</b>\n{failure_summary}"}})
            detailed_widgets.append({"textParagraph": {"text": f"<font color=\"#FF0000\"><b>Recent Failures:</b>\n{failure_list}</font>"}})

        payload = {
            "cardsV2": [{
                "cardId": f"job-summary-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": status_text, # Use the string with emoji char
                        "subtitle": datetime.now(local_timezone).strftime("%A %d %B, %H:%M"),
                        "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": [
                        {
                            "widgets": high_level_widgets
                        },
                        {
                            "header": "Detailed Metrics",
                            "collapsible": True,
                            "uncollapsibleWidgetsCount": 0, # Completely hidden until clicked
                            "widgets": detailed_widgets
                        }
                    ],
                },
            }]
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(chat_webhook_url, json=payload) as resp:
                if resp.status != 200:
                    app_logger.error(f"Job summary post failed: {resp.status}")

    except Exception as e:
        app_logger.error(f"Error posting job summary: {e}", exc_info=debug_mode)


async def post_performance_highlights(store_data: List[Dict[str, str]], chat_webhook_url: str,
                                      sanitize_func, local_timezone, debug_mode: bool, app_logger):
    """Send performance highlight cards. Kept Columns here for color emphasis on specific alerts."""
    if not chat_webhook_url or not store_data:
        return
    
    try:
        # Parse metrics from strings to numeric values
        parsed_stores = []
        for entry in store_data:
            try:
                # 1. ROBUST ORDER PARSING
                order_val = entry.get('orders', '0')
                if not order_val: continue
                if int(float(order_val)) == 0: continue

                # 2. SAFE METRIC PARSING
                def parse_metric(key, default_val='0'):
                    raw_str = entry.get(key, default_val)
                    clean_str = re.sub(r'[^0-9.]', '', raw_str)
                    return float(clean_str) if clean_str else 0.0

                lates_str = entry.get('lates', '0 %')
                lates_val = parse_metric('lates')
                
                inf_str = entry.get('inf', '0.0 %')
                inf_val = parse_metric('inf')
                
                uph_str = entry.get('uph', '0')
                uph_val = parse_metric('uph')
                
                parsed_stores.append({
                    'store': entry.get('store', 'Unknown'),
                    'lates': lates_val, 'lates_str': lates_str,
                    'inf': inf_val, 'inf_str': inf_str,
                    'uph': uph_val, 'uph_str': uph_str
                })
            except (ValueError, TypeError) as e:
                app_logger.warning(f"Could not parse metrics for {entry.get('store', 'Unknown')}: {e}")
                continue
        
        if not parsed_stores: return
        
        # Sort to find bottom performers
        sorted_by_lates = sorted(parsed_stores, key=lambda x: x['lates'], reverse=True)[:5]
        sorted_by_inf = sorted(parsed_stores, key=lambda x: x['inf'], reverse=True)[:5]
        sorted_by_uph = sorted(parsed_stores, key=lambda x: x['uph'])[:5]
        
        sections = []
        
        # Grid items logic for Highlights (Still uses Grid for clean lists, but with Emojis)
        def create_grid_section(title, stores, metric_key, metric_val_key):
            grid_items = [
                {"title": "Store", "textAlignment": "START"},
                {"title": metric_key, "textAlignment": "CENTER"},
            ]
            for store in stores:
                grid_items.extend([
                    {"title": sanitize_func(store['store']), "textAlignment": "START"},
                    {"title": f"❌ {store[metric_val_key]}", "textAlignment": "CENTER"},
                ])
            return {
                "header": f"⚠️ {title}",
                "widgets": [{"grid": {"title": title, "columnCount": 2, "borderStyle": {"type": "STROKE", "cornerRadius": 4}, "items": grid_items}}]
            }

        if sorted_by_lates and sorted_by_lates[0]['lates'] > 0:
            sections.append(create_grid_section("Highest Lates %", sorted_by_lates, "Lates %", "lates_str"))
        
        if sorted_by_inf and sorted_by_inf[0]['inf'] > 0:
            sections.append(create_grid_section("Highest INF %", sorted_by_inf, "INF %", "inf_str"))
            
        if sorted_by_uph:
             sections.append(create_grid_section("Lowest UPH", sorted_by_uph, "UPH", "uph_str"))
        
        if sections:
            payload = {
                "cardsV2": [{
                    "cardId": f"performance-highlights-{int(datetime.now().timestamp())}",
                    "card": {
                        "header": {
                            "title": "📊 Performance Highlights",
                            "subtitle": datetime.now(local_timezone).strftime("%A %d %B, %H:%M"),
                            "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                            "imageType": "CIRCLE"
                        },
                        "sections": sections,
                    },
                }]
            }
            
            timeout = aiohttp.ClientTimeout(total=30)
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.post(chat_webhook_url, json=payload) as resp:
                    if resp.status != 200:
                        app_logger.error(f"Performance highlights post failed: {resp.status}")

    except Exception as e:
        app_logger.error(f"Error posting performance highlights: {e}", exc_info=debug_mode)


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
        
        # Track submitted data for performance highlights
        async with submitted_data_lock:
            submitted_store_data.append(data)
        
        await add_to_chat_func(log_entry)