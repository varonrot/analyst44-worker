import os
import requests
from supabase import create_client

# Environment variables
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_SPY = "saifan_intraday_candles_spy_5m"


# ---------------------------------------------------------
# Fetch only last official 5-minute bar (OHLC)
# ---------------------------------------------------------
def fetch_last_spy_5m_bar():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    r = requests.get(url, timeout=10)
    data = r.json()

    if not isinstance(data, list) or len(data) == 0:
        print("[FMP] No data returned")
        return None

    # FMP returns newest bar first → sort ascending
    data_sorted = sorted(data, key=lambda x: x["date"])

    return data_sorted[-1]   # only newest official bar


# ---------------------------------------------------------
# Get last DB row
# ---------------------------------------------------------
def db_get_last_spy_row():
    resp = (
        supabase.table(TABLE_SPY)
        .select("*")
        .order("candle_time", desc=True)
        .limit(1)
        .execute()
    )
    return resp.data[0] if resp.data else None


# ---------------------------------------------------------
# UPSERT row
# ---------------------------------------------------------
def db_upsert_spy_row(payload):
    supabase.table(TABLE_SPY).upsert(
        payload,
        on_conflict=["symbol", "candle_time"]
    ).execute()

    print("[DB] UPSERT:", payload["candle_time"])


# ---------------------------------------------------------
# Compute indicators (VWAP, EMA12/26, MACD, etc.)
# ---------------------------------------------------------
def compute_indicators(new_bar, prev_row):
    close_price = float(new_bar["close"])
    high = float(new_bar["high"])
    low = float(new_bar["low"])
    volume = float(new_bar["volume"])

    typical_price = (high + low + close_price) / 3

    # Previous stored values
    if prev_row:
        cumulative_pv_prev = float(prev_row["cumulative_pv"])
        cumulative_vol_prev = float(prev_row["cumulative_vol"])

        ema12_prev = float(prev_row["ema12"])
        ema26_prev = float(prev_row["ema26"])
        signal_prev = float(prev_row["macd_signal"])
    else:
        cumulative_pv_prev = 0
        cumulative_vol_prev = 0
        ema12_prev = close_price
        ema26_prev = close_price
        signal_prev = 0

    # VWAP cumulative calculations
    cumulative_pv = cumulative_pv_prev + typical_price * volume
    cumulative_vol = cumulative_vol_prev + volume
    vwap = cumulative_pv / cumulative_vol if cumulative_vol > 0 else typical_price

    # EMA constants
    alpha12 = 2 / 13
    alpha26 = 2 / 27
    alpha9 = 2 / 10

    ema12 = ema12_prev + alpha12 * (close_price - ema12_prev)
    ema26 = ema26_prev + alpha26 * (close_price - ema26_prev)

    macd = ema12 - ema26
    macd_signal = signal_prev + alpha9 * (macd - signal_prev)
    macd_hist = macd - macd_signal

    return {
        "typical_price": typical_price,
        "cumulative_pv": cumulative_pv,
        "cumulative_vol": cumulative_vol,
        "vwap": vwap,
        "ema12": ema12,
        "ema26": ema26,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
    }


# ---------------------------------------------------------
# Main polling cycle
# ---------------------------------------------------------
def run_spy_cycle():
    print("[Saifan] Fetching last real SPY 5m bar...")

    new_bar = fetch_last_spy_5m_bar()
    if not new_bar:
        print("[Saifan] No new bar returned")
        return

    prev = db_get_last_spy_row()

    # Avoid duplicate insert
    if prev and prev["candle_time"] == new_bar["date"]:
        print("[Saifan] Bar already exists. Skipping.")
        return

    indicators = compute_indicators(new_bar, prev)

    # Build payload EXACT to table columns
    payload = {
        "symbol": "SPY",
        "candle_time": new_bar["date"],
        "open": float(new_bar["open"]),
        "high": float(new_bar["high"]),
        "low": float(new_bar["low"]),
        "close": float(new_bar["close"]),
        "volume": float(new_bar["volume"]),
        **indicators
    }

    db_upsert_spy_row(payload)
    print("[Saifan] SPY cycle completed.")
