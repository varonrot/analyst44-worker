import os
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

FMP_API_KEY = os.getenv("FMP_API_KEY")

TABLE = "saifan_intraday_vix_5m"


def round_to_5_minutes(dt: datetime):
    """Round current UTC time down to nearest 5-minute candle"""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def fetch_vix_quote():
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

        # === MAIN FIX: generate our own 5m candle timestamp ===
        candle_time = round_to_5_minutes(datetime.utcnow())

        row = {
            "symbol": "VIX",
            "candle_time": candle_time.isoformat(),
            "open": q.get("open"),
            "high": q.get("dayHigh"),
            "low": q.get("dayLow"),
            "close": q.get("price"),
            "volume": q.get("volume", 0),
        }

        supabase.table(TABLE).upsert(row).execute()
        print("[VIX] Live row inserted:", row)

    except Exception as e:
        print("[VIX ERROR] run_vix_cycle:", e)
