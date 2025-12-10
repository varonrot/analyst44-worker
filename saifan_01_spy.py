import os
import datetime
import requests
from supabase import create_client, Client

# =====================================
# CONFIG
# =====================================

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

TABLE_NAME = "saifan_intraday_candles_spy_5m"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# =====================================
# HELPERS
# =====================================

def round_to_5min(dt: datetime.datetime) -> datetime.datetime:
    """Round timestamp down to the nearest 5-minute block."""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def fetch_fmp_5m() -> list | None:
    """Fetch 5-minute SPY bars from FMP."""
    url = f"{FMP_URL}?apikey={FMP_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()

        if isinstance(data, dict):
            print("[FMP ERROR]:", data)
            return None

        if not isinstance(data, list) or not data:
            print("[FMP] No data returned.")
            return None

        return data

    except Exception as e:
        print("[FMP] Request error:", e)
        return None


def build_row_from_fmp_bar(bar: dict) -> dict:
    """Convert FMP bar into a DB row structure."""
    dt = datetime.datetime.strptime(bar["date"], "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    dt = round_to_5min(dt)

    return {
        "symbol": "SPY",
        "candle_time": dt.isoformat(),
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"],
    }


def upsert_bar(row: dict) -> None:
    """Insert or update bar into Supabase."""
    try:
        supabase.table(TABLE_NAME).upsert(row).execute()
        print(
            "[DB UPSERT]",
            row["candle_time"],
            "| close:", row["close"],
            "| volume:", row["volume"]
        )
    except Exception as e:
        print("[DB ERROR]:", e)


# =====================================
# MAIN CYCLE
# =====================================

def run_spy_cycle():
    """Main worker cycle: fetch → build → upsert (live + historical correction)."""

    print("=== [SPY] 5m LIVE CYCLE START ===")

    data = fetch_fmp_5m()
    if not data:
        print("[SPY] No data from FMP, aborting cycle.")
        return

    latest_bar = data[0]  # newest bar from FMP
    row = build_row_from_fmp_bar(latest_bar)

    upsert_bar(row)

    print("=== [SPY] 5m LIVE CYCLE END ===")


# =====================================
# LOCAL TEST
# =====================================

if __name__ == "__main__":
    run_spy_cycle()
