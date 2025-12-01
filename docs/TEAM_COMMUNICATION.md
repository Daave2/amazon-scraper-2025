# Team Communication Templates

## Google Chat Message

```
📊 **New Tool: Amazon Performance & INF Analysis Bot**

Hi team! 👋

I've launched an automated reporting system that delivers real-time store performance data directly to this chat.

**🎯 What This Tool Does:**
Provides data-driven insights to monitor stores and identify where support is needed. Stores already receive their daily update spreadsheet overnight - this tool gives you real-time visibility throughout the day.

**📅 Daily Automated Reports:**
• **8 AM** - Yesterday's INF Analysis
• **12 PM** - Performance Check
• **2 PM** - Today's INF Update  

**⚡ On-Demand Reports (Quick Actions):**
After each report, you'll see a "Quick Actions" card with buttons to instantly trigger:
• 🔍 INF Analysis (Today) - Top 10 missing items per store
• 📊 Performance Check - Lates, UPH, and key metrics
• 📅 Yesterday's INF Report - Items that were missed yesterday
• 📊 Week-to-Date INF - Monday through today summary

**📊 What's Included?**
✅ Performance highlights (Bottom 5 stores for Lates, INF, UPH)
✅ Detailed INF cards with product images, prices, stock levels, and QR codes
✅ Location data (aisle, bay, shelf)
✅ Discontinued product alerts

**How to Use:**
Just click the buttons! The system prevents duplicate requests (30-min cooldown).

Questions? Let me know!
```

---

## Email Template

**Subject:** Introducing Amazon Performance & INF Analysis Automation

**Body:**

Hi team,

I'm excited to introduce our new Amazon Performance & INF (Item Not Found) Analysis automation system.

### What Is It?

An automated system that scrapes Amazon Seller Central data for all Morrisons stores and delivers comprehensive reports directly to our Google Chat workspace. It runs multiple times daily and can be triggered on-demand with a single click.

**Context:**
Stores already receive their daily update spreadsheet (overnight batch). This tool provides real-time visibility throughout the day.

### Daily Automated Reports

The system sends 3 scheduled reports each day:

1. **8 AM - Yesterday's INF Analysis**
   - Complete breakdown of items that were marked "not found" yesterday
   - Top 10 most problematic items per store

2. **12 PM - Performance Check**
   - Current day performance metrics (Lates, INF, UPH)
   - Identifies stores that may need attention

3. **2 PM - Today's INF Update**
   - Mid-afternoon snapshot of current INF issues
   - Shows emerging problems before end-of-day

### On-Demand Reports (Quick Actions)

After each automated report, you'll see a "Quick Actions" card with 4 buttons:

- **INF Analysis (Today)** - Instant deep-dive into today's missing items
- **Performance Check** - Latest Lates, UPH, and performance metrics
- **Yesterday's INF Report** - Re-run yesterday's analysis
- **Week-to-Date INF** - Summary from Monday through today

Simply click any button to generate that report immediately. The system prevents duplicate requests (30-minute cooldown per report type) and notifies the chat who requested what.

### What's in the Reports?

**Performance Highlights:**
- Bottom 5 stores for Lates (highest late order percentage)
- Bottom 5 stores for INF (highest Item Not Found rate)
- Bottom 5 stores for UPH (lowest Units Per Hour)
- Job summary with throughput metrics and failure analysis

**Note:** The system currently tracks Lates, INF, and UPH. Available vs Confirmed picking hours (AvC) is not included at this time, as confirmed hours come from a separate sheet and available hours on the dashboard are unreliable in-day.

**Enhanced INF Cards:**
Each problematic item shows:
- High-resolution product image
- Product name and SKU
- Price (from Morrisons API)
- Current stock level and last updated time
- Location (aisle, bay, shelf)
- Promotional display location (if applicable)
- QR code for quick SKU lookup
- Discontinued/not-ranged alerts

**Network-Wide Summary:**
- Top 10 most problematic items across all stores
- Shows which stores contribute to each problem
- Example: "22 INF - VOSS Water (5 stores: Cleveleys 10, Woking 4, Halifax 3)"

### How to Use It

1. **Automated Reports** - No action needed! Reports arrive at scheduled times.
2. **On-Demand Reports** - Click the button for the report you want.
3. **Review Data** - Use the information to identify stores needing support and prioritize interventions.

### Benefits

✅ **Real-Time Visibility** - Multiple daily updates keep you informed throughout the day
✅ **Better Prioritization** - Focus on stores and items with the biggest impact
✅ **Reduced Manual Work** - No more manually checking each store individually
✅ **Team Visibility** - Everyone sees the same data at the same time
✅ **On-Demand Access** - Trigger any report instantly with Quick Actions

### Future Enhancements

We're exploring the possibility of sending INF item lists directly to individual store group chats (without Quick Actions access) to give them visibility into their specific issues.

### Support

If you have questions about the reports, need help interpreting data, or want to suggest improvements, please reach out!

The system is continuously being enhanced based on your feedback.

Best regards,
Niki

---

**P.S.** All data is pulled directly from Amazon Seller Central and enriched with real-time stock information from the Morrisons API. Reports typically complete in 3-5 minutes.
```

---

## Frequently Asked Questions

**Q: Can I trigger multiple reports at once?**
A: No, the system has a 30-minute cooldown per report type to prevent overload. You'll see a message if you try to trigger too soon.

**Q: How long does a report take?**
A: Most reports complete in 3-5 minutes. You'll see the results posted in chat automatically.

**Q: What if the report shows no data for my store?**
A: This usually means your store had 0 orders for that period, or there were no INF issues. Stores with 0 orders are automatically filtered out.

**Q: Can I change what time the automated reports run?**
A: The schedule is fixed (8 AM, 12 PM, 2 PM), but you can trigger any report on-demand using Quick Actions buttons.

**Q: What does "INF" mean?**
A: Item Not Found - when a customer orders an item but it can't be located during picking.

**Q: Who can I contact if something looks wrong?**
A: Contact Niki if you notice incorrect data or have questions about the reports.

**Q: What does the cooldown message mean?**
A: Someone already triggered that report recently. Wait until the cooldown period ends (shown in the message) before requesting again.

**Q: Will stores see these reports?**
A: Currently, these reports are for central and support teams only. Stores already receive their daily update spreadsheet. In the future, we may send INF item lists directly to store chats.

---

## How to Read the Reports

### Performance Highlights

- **🕒 Lates**: % of orders fulfilled late (lower is better)
  - Target: Keep as low as possible
  - Bottom 5 stores shown = stores with highest late %

- **⚠️ INF**: % of items not found (lower is better)
  - Target: Minimize items marked as not found
  - Bottom 5 stores shown = stores with highest INF rate

- **📦 UPH**: Units picked per hour (higher is better)
  - Target: Maximize picking efficiency
  - Bottom 5 stores shown = stores with lowest UPH

### INF Cards

- **Big bold number** = Total times this item was reported missing today/yesterday
- **📍 Location** = Where the item should be on the shelf (aisle, bay, shelf)
- **📊 Stock level** = How many are currently in stock (e.g., "10 EA" = 10 units)
- **💷 Price** = Current retail price
- **🚫 Red DISCONTINUED alert** = Product is no longer ranged (needs removal)
- **QR Code** = Scan with your device to look up SKU instantly

### Network-Wide Summary

Shows the top 10 most problematic items across ALL Morrisons stores:

Example: `22 - VOSS Water (5 stores: Cleveleys 10, Woking 4, Halifax 3) £1.25 | SKU: 12345`

- **22** = Total INF occurrences across all stores
- **5 stores** = Number of stores affected
- **Cleveleys 10** = Cleveleys had 10 INF for this item
- **£1.25** = Current price
- **SKU: 12345** = Product SKU for lookup

## Data Interpretation Guide

### INF Card Details

- **Big bold number** = Total times this item was reported missing for the period
- **📍 Location** = Aisle, bay, shelf information
- **📊 Stock level** = Current stock quantity (e.g., "10 EA" = 10 units)
- **💷 Price** = Current retail price
- **🚫 Red DISCONTINUED alert** = Product is no longer ranged
- **QR Code** = Scan to look up SKU

### Network-Wide Summary Format

Shows the top 10 most problematic items across ALL stores:

Example: `22 - VOSS Water (5 stores: Cleveleys 10, Woking 4, Halifax 3) £1.25 | SKU: 12345`

- **22** = Total INF occurrences across all stores
- **5 stores** = Number of stores affected
- **Cleveleys 10** = Cleveleys had 10 INF for this item
- **£1.25** = Current price
- **SKU: 12345** = Product SKU

### Quick Tips

- 💡 Network-wide summary helps identify systemic vs. store-specific issues
- 💡 Compare today's 2 PM report vs. yesterday's 8 AM report to track trends
- 💡 Check the "Last Updated" timestamp on stock data for freshness

---

## Mobile Cheat Sheet

*Screenshot this for quick reference on your phone!*

```
📱 AMAZON REPORTS BOT - QUICK REFERENCE

⏰ AUTOMATED SCHEDULE
🕗 8 AM  → Yesterday's INF
🕛 12 PM → Performance Check
🕑 2 PM  → Today's INF Update

⚡ QUICK ACTIONS BUTTONS (Click Anytime!)
🔍 Today's INF Analysis
📊 Performance Check
📅 Yesterday's INF Report
📊 Week-to-Date INF

📊 METRICS TO WATCH
🕒 Lates   ↓ (lower is better)
⚠️ INF     ↓ (lower is better)
📦 UPH     ↑ (higher is better)

⏱️ TIMING
• Reports take 3-5 minutes
• 30-minute cooldown per report type
• Acknowledgement posted immediately

💡 NEED HELP?
Contact: Niki
Location: Check cards for aisle/bay/shelf
Stock: Shows current quantity + unit


---

## Quick Reference Card (Optional - for pinning in chat)

```
🤖 **Amazon Reports Bot - Quick Reference**

**Automated Schedule:**
🕗 8 AM - Yesterday's INF
🕛 12 PM - Performance Check
🕑 2 PM - Today's INF

**Quick Actions Buttons:**
🔍 INF Analysis (Today)
📊 Performance Check
📅 Yesterday's INF
📊 Week-to-Date INF

**Cooldown:** 30 minutes per report type
**Response Time:** 3-5 minutes

💡 Tip: Click "Quick Actions" buttons after any report to trigger more analysis!
```
