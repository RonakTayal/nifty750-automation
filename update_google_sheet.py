import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================
# GOOGLE SHEETS AUTH
# ==============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

import os
import json

creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

spreadsheet = client.open("NIFTY750")

raw_sheet = spreadsheet.worksheet("RAW")
mapping_sheet = spreadsheet.worksheet("MAPPING")

# ==============================
# LOAD STOCK LIST
# ==============================
mapping = pd.DataFrame(mapping_sheet.get_all_records())

# ==============================
# FETCH DATA
# ==============================
new_rows = []

for i in range(len(mapping)):
    symbol = mapping.iloc[i]["Symbol"]
    name = mapping.iloc[i]["Name"]

    try:
        data = yf.download(symbol, period="5d", interval="1d", progress=False)

        if data.empty:
            print(f"No data for {symbol}")
            continue

        # Fix for yfinance MultiIndex issue
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        latest = data.tail(1)

        date_val = latest.index[0].date()

        open_price = float(latest["Open"].iloc[0])
        high_price = float(latest["High"].iloc[0])
        low_price = float(latest["Low"].iloc[0])
        close_price = float(latest["Close"].iloc[0])

        row = [
            str(date_val),
            symbol,
            open_price,
            high_price,
            low_price,
            close_price,
            name
        ]

        new_rows.append(row)
        print(f"Done: {symbol}")

    except Exception as e:
        print(f"Error in {symbol}: {e}")

# ==============================
# APPEND TO GOOGLE SHEET
# ==============================
if new_rows:
    raw_sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
    print("✅ Data appended successfully!")
else:
    print("⚠️ No data to upload")