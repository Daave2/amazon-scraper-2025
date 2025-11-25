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
        numeric_value = float(re.sub(r'[^\d.]', '', value_str))
        is_good = (numeric_value >= threshold) if is_uph else (numeric_value <= threshold)
        emoji = emoji_green if is_good else emoji_red
        return f"{emoji} {value_str}"
    except (ValueError, TypeError):
        return value_str # Return as is if not a number


async def post_to_chat_webhook(entries: List[Dict[str, str]], chat_webhook_url: str,
                               chat_batch_count: int, get_date_range_func, sanitize_func,
                               uph_threshold: float, lates_threshold: float, inf_threshold: float,
                               emoji_green: str, emoji_red: str, local_timezone, debug_mode: bool, app_logger):
    """Send a table-formatted card message with emoji indicators."""
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
        entries = [e for e in entries if int(e.get('orders', '0')) > 0]

        sorted_entries = sorted(entries, key=lambda e: sanitize_func(e.get("store", "")))

        # --- Build the Grid/Table Widget with Emoji Indicators ---
        grid_items = [
            {"title": "Store", "textAlignment": "START"},
            {"title": "UPH", "textAlignment": "CENTER"},
            {"title": "Lates", "textAlignment": "CENTER"},
            {"title": "INF", "textAlignment": "CENTER"},
        ]

        for entry in sorted_entries:
            uph_val = entry.get("uph", "N/A")
            lates_val = entry.get("lates", "0.0 %") or "0.0 %"
            inf_val = entry.get("inf", "0.0 %") or "0.0 %"

            # Apply emoji formatting
            formatted_uph = _format_metric_with_emoji(uph_val, uph_threshold, emoji_green, emoji_red, is_uph=True)
            formatted_lates = _format_metric_with_emoji(lates_val, lates_threshold, emoji_green, emoji_red)
            formatted_inf = _format_metric_with_emoji(inf_val, inf_threshold, emoji_green, emoji_red)

            grid_items.extend([
                {"title": sanitize_func(entry.get("store", "N/A")), "textAlignment": "START"},
                {"title": formatted_uph, "textAlignment": "CENTER"},
                {"title": formatted_lates, "textAlignment": "CENTER"},
                {"title": formatted_inf, "textAlignment": "CENTER"},
            ])
        
        table_section = {
            "header": "Key Performance Indicators",
            "widgets": [{
                "grid": {
                    "title": "Performance Summary",
                    "columnCount": 4,
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
    """Send a detailed job summary card to Google Chat with advanced analytics."""
    if not chat_webhook_url: return
    try:
        status_text = "Job Completed Successfully"
        status_icon = "✅"
        if failures:
            status_text = f"Job Completed with {len(failures)} Failures"
            status_icon = "⚠️"
        
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
        avg_sub = sum(t[1] for t in sub_times) / len(sub_times) if sub_times else 0
        
        # P95 Latency
        sorted_coll = sorted([t[1] for t in coll_times])
        p95_coll = sorted_coll[int(len(sorted_coll) * 0.95)] if sorted_coll else 0
        
        fastest_store = min(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        slowest_store = max(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        
        # Bottleneck Analysis
        bottleneck_msg = "Balanced Flow"
        if avg_coll > 2.0:
            bottleneck_msg = "🐢 Slow Scraping (Browser Lag)"
        elif avg_sub > 1.0:
            bottleneck_msg = "🐢 Slow Submission (Webhook Lag)"
        elif avg_coll < 1.0 and avg_sub < 0.5:
            bottleneck_msg = "🚀 High Speed (No Bottlenecks)"

        # Build sections
        sections = [
            {
                "header": "High-Level Stats",
                "widgets": [
                    {"decoratedText": {"topLabel": "Throughput", "text": f"{throughput_spm:.1f} stores/min", "startIcon": {"knownIcon": "FLIGHT_DEPARTURE"}}},
                    {"decoratedText": {"topLabel": "Success Rate", "text": f"{success}/{total} ({success_rate:.1f}%)", "startIcon": {"knownIcon": "STAR"}}},
                    {"decoratedText": {"topLabel": "Total Duration", "text": f"{duration:.2f}s", "startIcon": {"knownIcon": "CLOCK"}}}
                ]
            },
            {
                "header": "Business Volume 📦",
                "widgets": [
                    {"decoratedText": {"topLabel": "Total Orders", "text": f"{total_orders:,}", "startIcon": {"knownIcon": "SHOPPING_CART"}}},
                    {"decoratedText": {"topLabel": "Total Units", "text": f"{total_units:,}", "startIcon": {"knownIcon": "TICKET"}}}
                ]
            },
            {
                "header": "Resilience & Health 🏥",
                "widgets": [
                    {"decoratedText": {"topLabel": "Total Retries", "text": str(retries), "startIcon": {"knownIcon": "MEMBERSHIP"}}},
                    {"decoratedText": {"topLabel": "Stores Retried", "text": str(retry_stores), "startIcon": {"knownIcon": "STORE"}}}
                ]
            },
            {
                "header": "Speed Breakdown ⏱️",
                "widgets": [
                    {"decoratedText": {"topLabel": "Avg Collection Time", "text": f"{avg_coll:.2f}s (Browser)", "startIcon": {"knownIcon": "DESCRIPTION"}}},
                    {"decoratedText": {"topLabel": "p95 Collection Time", "text": f"{p95_coll:.2f}s", "startIcon": {"knownIcon": "DESCRIPTION"}}},
                    {"decoratedText": {"topLabel": "Bottleneck Status", "text": bottleneck_msg, "startIcon": {"knownIcon": "TRAFFIC"}}}
                ]
            },
            {
                "header": "Extremes 📉📈",
                "widgets": [
                    {"decoratedText": {"topLabel": "Fastest Store", "text": f"{fastest_store[0]} ({fastest_store[1]:.2f}s)", "startIcon": {"knownIcon": "BOLT"}}},
                    {"decoratedText": {"topLabel": "Slowest Store", "text": f"{slowest_store[0]} ({slowest_store[1]:.2f}s)", "startIcon": {"knownIcon": "SNAIL"}}}
                ]
            }
        ]
        
        if failures:
            # Group failures by type
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
            
            sections.append({
                "header": "Failure Analysis",
                "widgets": [
                    {"textParagraph": {"text": f"<b>Breakdown:</b>\n{failure_summary}"}},
                    {"textParagraph": {"text": f"<font color=\"#FF0000\"><b>Recent Failures:</b>\n{failure_list}</font>"}}
                ]
            })

        payload = {
            "cardsV2": [{
                "cardId": f"job-summary-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": f"{status_icon} {status_text}",
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
                    app_logger.error(f"Job summary post failed: {resp.status}")

    except Exception as e:
        app_logger.error(f"Error posting job summary: {e}", exc_info=debug_mode)


async def post_performance_highlights(store_data: List[Dict[str, str]], chat_webhook_url: str,
                                      sanitize_func, local_timezone, debug_mode: bool, app_logger):
    """Send performance highlight cards for bottom performers."""
    if not chat_webhook_url or not store_data:
        return
    
    try:
        # Parse metrics from strings to numeric values
        parsed_stores = []
        for entry in store_data:
            try:
                # Filter out stores with 0 orders
                if int(entry.get('orders', '0')) == 0:
                    continue

                lates_str = entry.get('lates', '0 %')
                lates_val = float(re.sub(r'[^0-9.]', '', lates_str))
                inf_str = entry.get('inf', '0.0 %')
                inf_val = float(re.sub(r'[^0-9.]', '', inf_str))
                uph_str = entry.get('uph', '0')
                uph_val = float(re.sub(r'[^0-9.]', '', uph_str))
                
                parsed_stores.append({
                    'store': entry.get('store', 'Unknown'),
                    'lates': lates_val, 'lates_str': lates_str,
                    'inf': inf_val, 'inf_str': inf_str,
                    'uph': uph_val, 'uph_str': uph_str
                })
            except (ValueError, TypeError) as e:
                app_logger.warning(f"Could not parse metrics for {entry.get('store', 'Unknown')}: {e}")
                continue
        
        if not parsed_stores:
            app_logger.warning("No valid store data to create performance highlights")
            return
        
        # Sort to find bottom performers
        sorted_by_lates = sorted(parsed_stores, key=lambda x: x['lates'], reverse=True)[:5]
        sorted_by_inf = sorted(parsed_stores, key=lambda x: x['inf'], reverse=True)[:5]
        sorted_by_uph = sorted(parsed_stores, key=lambda x: x['uph'])[:5]
        
        # Build cards
        sections = []
        
        # Card 1: Bottom 5 Lates
        if sorted_by_lates:
            lates_grid_items = [
                {"title": "Store", "textAlignment": "START"},
                {"title": "Lates %", "textAlignment": "CENTER"},
            ]
            for store in sorted_by_lates:
                lates_grid_items.extend([
                    {"title": sanitize_func(store['store']), "textAlignment": "START"},
                    {"title": f"❌ {store['lates_str']}", "textAlignment": "CENTER"},
                ])
            
            sections.append({
                "header": "⚠️ Bottom 5 Stores: Highest Lates %",
                "widgets": [{"grid": {"title": "Stores with Highest Late Percentages", "columnCount": 2, "borderStyle": {"type": "STROKE", "cornerRadius": 4}, "items": lates_grid_items}}]
            })
        
        # Card 2: Bottom 5 INF
        if sorted_by_inf:
            inf_grid_items = [
                {"title": "Store", "textAlignment": "START"},
                {"title": "INF %", "textAlignment": "CENTER"},
            ]
            for store in sorted_by_inf:
                inf_grid_items.extend([
                    {"title": sanitize_func(store['store']), "textAlignment": "START"},
                    {"title": f"❌ {store['inf_str']}", "textAlignment": "CENTER"},
                ])
            
            sections.append({
                "header": "⚠️ Bottom 5 Stores: Highest INF %",
                "widgets": [{"grid": {"title": "Stores with Highest Item Not Found Rate", "columnCount": 2, "borderStyle": {"type": "STROKE", "cornerRadius": 4}, "items": inf_grid_items}}]
            })
        
        # Card 3: Bottom 5 UPH
        if sorted_by_uph:
            uph_grid_items = [
                {"title": "Store", "textAlignment": "START"},
                {"title": "UPH", "textAlignment": "CENTER"},
            ]
            for store in sorted_by_uph:
                uph_grid_items.extend([
                    {"title": sanitize_func(store['store']), "textAlignment": "START"},
                    {"title": f"❌ {store['uph_str']}", "textAlignment": "CENTER"},
                ])
            
            sections.append({
                "header": "⚠️ Bottom 5 Stores: Lowest UPH",
                "widgets": [{"grid": {"title": "Stores with Lowest Units Per Hour", "columnCount": 2, "borderStyle": {"type": "STROKE", "cornerRadius": 4}, "items": uph_grid_items}}]
            })
        
        # Send the card
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
                else:
                    app_logger.info("Performance highlights posted successfully")

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
