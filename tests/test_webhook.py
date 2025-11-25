import pytest
import logging
from unittest.mock import MagicMock, AsyncMock, patch
from webhook import post_performance_highlights, post_to_chat_webhook, _format_metric_with_emoji

# Mock logger
logger = logging.getLogger("test_logger")
logger.setLevel(logging.INFO)

# Mock sanitize function
def sanitize(text):
    return text

@pytest.mark.parametrize("value, threshold, is_uph, expected_emoji", [
    ("90", 80, True, "✅"),   # UPH >= 80 (Good)
    ("70", 80, True, "❌"),   # UPH < 80 (Bad)
    ("2.0 %", 3.0, False, "✅"), # Lates <= 3.0 (Good)
    ("5.0 %", 3.0, False, "❌"), # Lates > 3.0 (Bad)
    ("1.0 %", 2.0, False, "✅"), # INF <= 2.0 (Good)
    ("3.0 %", 2.0, False, "❌"), # INF > 2.0 (Bad)
])
def test_format_metric_with_emoji(value, threshold, is_uph, expected_emoji):
    result = _format_metric_with_emoji(value, threshold, "✅", "❌", is_uph=is_uph)
    assert expected_emoji in result

@pytest.mark.asyncio
async def test_post_performance_highlights_filtering():
    """Test that stores with 0 orders are filtered out."""
    store_data = [
        {'store': 'Store A', 'orders': '10', 'lates': '5.0 %', 'inf': '2.0 %', 'uph': '100'},
        {'store': 'Store B', 'orders': '0', 'lates': '10.0 %', 'inf': '10.0 %', 'uph': '50'}, # Should be filtered
    ]
    
    with patch('aiohttp.ClientSession.post') as mock_post:
        mock_post.return_value.__aenter__.return_value.status = 200
        
        await post_performance_highlights(
            store_data=store_data,
            chat_webhook_url="http://mock-url",
            sanitize_func=sanitize,
            local_timezone=None,
            debug_mode=True,
            app_logger=logger
        )
        
        args, kwargs = mock_post.call_args
        json_payload = kwargs['json']
        
        # Check that Store B is NOT in the payload
        payload_str = str(json_payload)
        assert "Store A" in payload_str
        assert "Store B" not in payload_str

@pytest.mark.asyncio
async def test_post_performance_highlights_sorting():
    """Test bottom 5 sorting logic."""
    store_data = [
        {'store': 'Store A', 'orders': '10', 'lates': '1.0 %', 'inf': '1.0 %', 'uph': '100'},
        {'store': 'Store B', 'orders': '10', 'lates': '10.0 %', 'inf': '10.0 %', 'uph': '10'}, # Worst
        {'store': 'Store C', 'orders': '10', 'lates': '5.0 %', 'inf': '5.0 %', 'uph': '50'},
    ]
    
    with patch('aiohttp.ClientSession.post') as mock_post:
        mock_post.return_value.__aenter__.return_value.status = 200
        
        await post_performance_highlights(
            store_data=store_data,
            chat_webhook_url="http://mock-url",
            sanitize_func=sanitize,
            local_timezone=None,
            debug_mode=True,
            app_logger=logger
        )
        
        args, kwargs = mock_post.call_args
        json_payload = kwargs['json']
        sections = json_payload['cardsV2'][0]['card']['sections']
        
        # Helper to extract items from a section title
        def get_items_for_section(title_part):
            for section in sections:
                if title_part in section.get('header', ''):
                    return section['widgets'][0]['grid']['items']
            return []

        # Check Lates (Highest is bad) -> Store B should be first
        lates_items = get_items_for_section("Highest Lates")
        # Items structure: Header1, Header2, Store1, Value1, Store2, Value2...
        assert lates_items[2]['title'] == 'Store B'
        
        # Check INF (Highest is bad) -> Store B should be first
        inf_items = get_items_for_section("Highest INF")
        assert inf_items[2]['title'] == 'Store B'
        
        # Check UPH (Lowest is bad) -> Store B should be first
        uph_items = get_items_for_section("Lowest UPH")
        assert uph_items[2]['title'] == 'Store B'

@pytest.mark.asyncio
async def test_post_to_chat_webhook_filtering():
    """Test that stores with 0 orders are filtered out from batch summary."""
    entries = [
        {'store': 'Store A', 'orders': '10', 'lates': '0%', 'inf': '0%', 'uph': '100'},
        {'store': 'Store B', 'orders': '0', 'lates': '0%', 'inf': '0%', 'uph': '0'},
    ]
    
    with patch('aiohttp.ClientSession.post') as mock_post:
        mock_post.return_value.__aenter__.return_value.status = 200
        
        await post_to_chat_webhook(
            entries=entries,
            chat_webhook_url="http://mock-url",
            chat_batch_count=1,
            get_date_range_func=lambda: None,
            sanitize_func=sanitize,
            uph_threshold=80,
            lates_threshold=3.0,
            inf_threshold=2.0,
            emoji_green="✅",
            emoji_red="❌",
            local_timezone=None,
            debug_mode=True,
            app_logger=logger
        )
        
        args, kwargs = mock_post.call_args
        json_payload = kwargs['json']
        payload_str = str(json_payload)
        
        assert "Store A" in payload_str
        assert "Store B" not in payload_str
