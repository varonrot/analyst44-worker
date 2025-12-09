import os
import requests
from datetime import datetime, timezone
import supabase

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
FMP_API_KEY = os.environ["FMP_API_KEY"]

supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)
TABLE_NAME = "saifan_intraday_candles_spy_5m"


def fetch_last_5m_bar():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    print("[SPY] Fetching:", url)

    resp = requests.get(url, timeout=10)
    data = resp.json()

    if isinstance(data, list) and len(data) > 0:
        return data[0]

    print("[SPY ERROR] FMP returned no data:", data)
    return None


def run_spy_cycle():
    print("[SPY] Running SPY 5m cycle...")

    new_bar = fetch_last_5m_bar()
    if not new_bar:
        print("[SPY ERROR] No bar received.")
        return

    try:
        o = float(new_bar["open"])
        h = float(new_bar["high"])
        l = float(new_bar["low"])
        c = float(new_bar["close"])
        v = float(new_bar["volume"])

        dt = datetime.strptime(new_bar["date"], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        candle_time = dt.isoformat()

        payload = {
            "symbol": "SPY",
            "candle_time": candle_time,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
        }

        res = (
            supabase_client.table(TABLE_NAME)
            .upsert(payload, on_conflict=["symbol", "candle_time"])
            .execute()
        )

        print("[SPY] Row saved:", payload)

    except Exception as e:
        print("[SPY ERROR] Failed to process bar:", e)
