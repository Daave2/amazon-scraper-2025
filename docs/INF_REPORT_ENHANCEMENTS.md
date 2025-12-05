# INF Report Card Enhancements

## Summary
Enhanced the Google Chat INF report cards with improved layout, higher resolution images, and QR codes for quick SKU lookup.

## Changes Made

### 1. **Higher Resolution Product Images**
- **Before**: 75px thumbnail images (`_SL75_`, `_AC_UL50_`)
- **After**: 300px high-resolution images (`_SL300_`, `_AC_UL300_`)
- **Impact**: 4x clearer product images for better visual identification

### 2. **Layout Redesign - Images Moved to Right**
**Old Layout:**
```
┌─────────────────────────────────┐
│ [Image]  │  Text Details        │
└─────────────────────────────────┘
```

**New Layout:**
```
┌─────────────────────────────────────────┐
│ Text Details      │  [Product Image]    │
│                   │                      │
│ 📦 SKU: 112571916│  [QR Code]          │
│ ⚠️ INF Units: 15 │  Scan to lookup SKU  │
│ 📊 Stock: 10 EA  │                      │
│ 📍 Aisle 5, Bay 3│                      │
└─────────────────────────────────────────┘
```

**Benefits:**
- Text on left is easier to scan (Western reading order)
- Images grouped together on right for better visual hierarchy
- More space for detailed information

### 3. **QR Code Generation**
- **Added**: QR code for each SKU below the product image
- **Use Case**: Warehouse staff can scan QR codes to quickly look up SKU in their systems
- **Format**: Base64-encoded data URL embedded directly in the card
- **Label**: "Scan to lookup SKU" text above QR code

### 4. **Enhanced Text Formatting**
- **Emojis**: Added visual icons for better scanning
  - 📦 for SKU
  - ⚠️ for INF Units
  - 📊 for Stock levels
  - 📍 for Location
- **Color Coding**: SKU highlighted in blue (`#1a73e8`)
- **Bold Text**: Product name and INF count emphasized
- **Better Spacing**: Added line breaks for improved readability

### 5. **Stock Location Integration**
- Now displays standard location (e.g., "Aisle 5, Left bay 3, shelf 2") when available
- Helps warehouse staff quickly locate items causing INF issues

## Technical Implementation

### New Dependencies
```
qrcode[pil]  # For QR code generation with PIL image support
```

### New Function: `generate_qr_code_data_url()`
```python
def generate_qr_code_data_url(sku: str) -> str:
    """Generate a QR code as a data URL for embedding in Google Chat."""
```

- Creates QR codes from SKU strings
- Returns base64-encoded PNG as data URL
- Handles errors gracefully (returns empty string on failure)

### Column Layout Update
```python
columnItems = [
    {
        "horizontalSizeStyle": "FILL_AVAILABLE_SPACE",  # Text takes available space
        "horizontalAlignment": "START",                  # Left-aligned
        "verticalAlignment": "TOP",
        "widgets": [text_details]
    },
    {
        "horizontalSizeStyle": "FILL_MINIMUM_SPACE",    # Images take minimum needed
        "horizontalAlignment": "END",                    # Right-aligned  
        "verticalAlignment": "TOP",
        "widgets": [product_image, qr_label, qr_code]
    }
]
```

## Example Output

For a product with SKU "112571916", the card now shows:

**Left Column:**
```
Morrisons Bananas Loose

📦 SKU: 112571916
⚠️ INF Units: 15
📊 Stock: 10 EA
📍 Aisle 5, Left bay 3, shelf 2
```

**Right Column:**
```
[Product Image - 300x300px]

Scan to lookup SKU
[QR Code of "112571916"]
```

## Testing

Run the test script to verify functionality:
```bash
python3 test_inf_report_enhancements.py
```

This validates:
- ✅ QR code generation from SKUs
- ✅ Image URL enhancement (75px → 300px)
- ✅ Layout structure

## Deployment

### Local Testing
1. Install dependencies: `pip3 install 'qrcode[pil]'`
2. Add `inventory_system_url` to `config.json` (optional)
3. Run INF scraper to test the new layout
4. Check Google Chat for enhanced cards

### GitHub Actions
Dependencies automatically installed via `requirements.txt` during workflow run.

## Implemented Features

### Clickable Inventory Links ✅
- **Added**: Clickable "Info" button below each QR code
- **Configuration**: Set `inventory_system_url` in `config.json`
  - Example: `"inventory_system_url": "https://amazon-product-analysis-584939250419.us-west1.run.app/assistant/{sku}?locationId={store_number}"`
  - The `{sku}` and `{store_number}` placeholders are automatically replaced
- **Benefit**: One-click access to detailed inventory information in Focal Systems IMS
- **User Experience**: Each INF card now has both a scannable QR code (for mobile) and a clickable button (for desktop)

## Future Enhancements

Potential improvements for future consideration:
- **Dynamic QR size**: Adjust QR code size based on screen size
- **Color-coded stock levels**: Red for low stock, green for healthy stock
- **Trend indicators**: Show if INF is increasing or decreasing
- **Action buttons**: "Mark as resolved", "Request restock", etc.
