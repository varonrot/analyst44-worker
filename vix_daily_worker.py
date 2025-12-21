import os
import requests
from datetime import datetime, timedelta
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
DAYS_BACK = 180   # חצי שנה
SOURCE = "FMP"

# ======================
# FETCH VIX DATA
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
# UPSERT INTO SUPABASE
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
    print("Fetching VIX historical data...")
    rows = fetch_vix_history()
    print(f"Fetched {len(rows)} rows")

    print("Upserting into vix_daily...")
    upsert_vix_rows(rows)

    print("VIX daily update completed successfully")

if __name__ == "__main__":
    run()
