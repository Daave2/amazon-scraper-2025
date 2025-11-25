import csv

# Store number mapping from user's data
store_numbers = {
    "Acton": "302",
    "Baglan Moor": "96",
    "Banbury": "106",
    "Basingstoke": "345",
    "Becontree Heath": "296",
    "Bedford": "628",
    "Binley": "69",
    "Bristol": "84",
    "Bromsgrove": "242",
    "Bulwell": "121",
    "Cambourne": "118",
    "Canning Town": "191",
    "Canterbury": "293",
    "Canvey Island": "276",
    "Cardiff": "269",
    "Chingford": "1",
    "Chippenham": "348",
    "Coalville": "54",
    "Corby": "232",
    "Croydon": "576",
    "Derby": "78",
    "Eastbourne": "546",
    "Ebbw Vale": "95",
    "Exeter": "496",
    "Gloucester": "575",
    "Gravesend": "298",
    "High Wycombe": "300",
    "Ipswich": "81",
    "Leicester": "234",
    "Maidstone": "315",
    "Milton Keynes": "154",  # MK Plaza
    "Newport": "407",
    "Northampton": "100",
    "Norwich": "115",
    "Oxted": "356",
    "Peckham": "306",
    "Plymouth": "333",  # Plymstock
    "Portsmouth": "522",
    "Queensbury": "307",
    "Reading": "359",
    "Redruth": "334",
    "Sheldon": "554",
    "Stanground": "414",  # Peterborough
    "Stratford": "309",
    "Swindon": "634",
    "Taunton": "335",
    "Totnes": "339",
    "Totton": "364",
    "Walsall": "63",  # Bescot
    "Warminster": "340",
    "Watford": "586",
    "Welling": "447",
    "Weston Super Mare": "341",
    "Weybridge": "404",
    "Welwyn Garden City": "319",
    "Wisbech": "511",
    "Witham": "290",
    "Woking": "367",
    "Worthing": "641",
    "Aberdeen": "160",
    "Anlaby": "107",  # Hull - Anlaby isn't in urls.csv, but Hull is
    "Anniesland": "179",
    "Belle Vale": "97",
    "Boroughbridge": "108",
    "Bishop Auckland": "40",
    "Byker": "91",
    "Cardonald": "136",
    "Catcliffe": "29",  # Sheffield - Catcliffe isn't in urls.csv
    "Cleethorpes": "75",
    "Connahs Quay": "440",
    "Dundee": "145",
    "Eccles": "28",
    "Glenrothes": "184",
    "Gyle": "177",
    "Halifax": "475",
    "Hunslet": "15",
    "Lincoln": "62",
    "Middlesbrough": "128",
    "Newark": "48",
    "Preston": "41",
    "Reddish": "216",
    "Rhyl": "129",
    "St. Helens": "21",
    "Staveley": "27",
    "Stevenston": "196",
    "Stirling": "197",
    "Stoke": "50",
    "Thornbury": "8",
    "Thornton-Cleveleys": "218",
    "Wellington": "49",
    "Winsford": "77",
    "Wrexham": "417",
    "York": "155",
    "Nelson": "104",
    "Gorleston": "439",
    "Harrow": "549",
    "Verwood": "640",
    "Jarrow": "88",
    "Kirkstall": "89",
}

# Read existing CSV
with open('urls.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Add store numbers
for row in rows:
    store_name = row['store_name'].replace('Morrisons - ', '')
    row['store_number'] = store_numbers.get(store_name, '')

# Write updated CSV with new column order
with open('urls.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['store_number', 'merchant_id', 'new_id', 'store_name', 'marketplace_id']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print("Store numbers added successfully!")
print("Stores without numbers:")
for row in rows:
    if not row['store_number']:
        print(f"  - {row['store_name']}")
