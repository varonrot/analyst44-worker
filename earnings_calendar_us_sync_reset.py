import os
import requests
from supabase import create_client
from datetime import datetime
from dotenv import load_dotenv

# ==========================================
# Load environment (.env should contain:)
# SUPABASE_URL=
# SUPABASE_SERVICE_ROLE_KEY=
# FMP_API_KEY=
# ==========================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

TABLE_NAME = "earnings_calendar_us"


# ------------------------------------------
# Fetch earnings data from FMP
# ------------------------------------------
def fetch_calendar_from_fmp():
    base_url = "https://financialmodelingprep.com/api/v3/earning_calendar"

    # ✅ ONLY TODAY — no range, only today's date
    today = datetime.today().strftime("%Y-%m-%d")

    url = f"{base_url}?from={today}&to={today}&apikey={FMP_API_KEY}"
    print(f"🔎 Fetching earnings ONLY for today:\n{url}\n")

    response = requests.get(url)
    response.raise_for_status()
    return response.json()


# ------------------------------------------
# Delete all data in table
# ------------------------------------------
def delete_table_data():
    print("🗑  Deleting existing rows...")

    res = supabase.table(TABLE_NAME).delete().neq("symbol", "").execute()
    deleted = res.count if hasattr(res, "count") else 0

    print(f"✅ Deleted rows: {deleted}")


# ------------------------------------------
# Insert (bulk) new records
# ------------------------------------------
def insert_new_rows(data):
    print("\n📥 Inserting new records...")

    chunk_size = 200
    total_inserted = 0

    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        supabase.table(TABLE_NAME).insert(chunk).execute()
        total_inserted += len(chunk)
        print(f"   ✅ chunk inserted: {len(chunk)}")

    print(f"✅ Total inserted: {total_inserted}")


# ------------------------------------------
# MAIN PROCESS
# ------------------------------------------
def main():
    print("\n===== Earnings Calendar Sync (FULL RESET — TODAY ONLY, US ONLY) =====\n")

    calendar = fetch_calendar_from_fmp()

    print(f"📊 Raw records returned by FMP: {len(calendar)}")

    # Extract only relevant fields
    rows = []
    for item in calendar:
        rows.append({
            "symbol": item.get("symbol"),
            "date": item.get("date"),
            "eps": item.get("eps"),
            "eps_estimated": item.get("epsEstimated"),
            "revenue": item.get("revenue"),
            "time": item.get("time"),
        })

    # ✅ Remove non-US: no "." AND symbol length ≤ 4 (avoids OTC)
    rows = [
        item for item in rows
        if item["symbol"] and "." not in item["symbol"] and len(item["symbol"]) <= 4
    ]

    print(f"🇺🇸 After US filter (no dot + ≤4 chars): {len(rows)}")

    delete_table_data()
    insert_new_rows(rows)

    print("\n✅ DONE — table reset & refreshed ✅")
    print("=====================================================\n")


if __name__ == "__main__":
    main()
