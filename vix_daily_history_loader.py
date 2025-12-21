import os
import requests
from datetime import datetime
from supabase import create_client, Client

# ======================
# ENV
# ======================
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([FMP_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    raise Exception("Missing environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======================
# CONFIG
# ======================
SYMBOL = "^VIX"
DAYS_BACK = 180
SOURCE = "FMP"

# ======================
# STEP 1.16 – RESET TABLE (CORRECT WAY)
# ======================
def reset_vix_table():
    print("Resetting vix_daily table...")

    supabase.table("vix_daily") \
        .delete() \
        .neq("trade_date", "1900-01-01") \
        .execute()

    print("vix_daily table reset completed")

# ======================
# FETCH DATA
# ======================
def fetch_vix_history():
    url = (
        f"https://financialmodelingprep.com/api/v3/historical-price-full/"
        f"{SYMBOL}?timeseries={DAYS_BACK}&apikey={FMP_API_KEY}"
    )

    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    if "historical" not in data:
        raise Exception("No historical data returned from FMP")

    return data["historical"]

# ======================
# UPSERT
# ======================
def upsert_vix_rows(rows):
    for row in rows:
        trade_date = datetime.strptime(row["date"], "%Y-%m-%d").date()

        payload = {
            "trade_date": trade_date.isoformat(),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "source": SOURCE,
        }

        supabase.table("vix_daily").upsert(
            payload,
            on_conflict="trade_date"
        ).execute()

# ======================
# MAIN
# ======================
def run():
    print("=== VIX DAILY WORKER START ===")

    reset_vix_table()          # Step 1.16
    rows = fetch_vix_history()
    print(f"Fetched {len(rows)} VIX rows")

    upsert_vix_rows(rows)

    print("=== VIX DAILY WORKER DONE ===")

if __name__ == "__main__":
    run()
