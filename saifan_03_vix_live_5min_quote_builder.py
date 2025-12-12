import os
import requests
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

FMP_API_KEY = os.getenv("FMP_API_KEY")

TABLE = "saifan_intraday_vix_5m"


def fetch_vix_quote():
    """Fetch latest VIX data (5-minute quote snapshot)"""
    url = f"https://financialmodelingprep.com/api/v3/quote/%5EVIX?apikey={FMP_API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Failed fetching VIX quote")

    data = r.json()
    if not data:
        raise Exception("Empty VIX response")

    return data[0]


def run_vix_cycle():
    """Insert live VIX data into Supabase (REAL-TIME mode)"""
    try:
        q = fetch_vix_quote()

        candle_time = q.get("timestamp")
        if candle_time is None:
            raise Exception("Missing timestamp from VIX quote")

        row = {
            "candle_time": candle_time,
            "open": q.get("open"),
            "high": q.get("dayHigh"),
            "low": q.get("dayLow"),
            "close": q.get("price"),
            "volume": q.get("volume"),
        }

        supabase.table(TABLE).upsert(row).execute()
        print("[VIX] Live row inserted:", row)

    except Exception as e:
        print("[VIX ERROR] run_vix_cycle:", e)
