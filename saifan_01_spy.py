# ======================================================
# FIXED saifan_01_spy.py — FULL WORKING VERSION
# ======================================================

import os
import requests
from supabase import create_client

FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_SPY = "saifan_intraday_candles_spy_5m"


# ----------------------------------------
# Fetch SPY
# ----------------------------------------
def fetch_spy_5m():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    res = requests.get(url, timeout=10)
    data = res.json()
    return data[0] if isinstance(data, list) and len(data) > 0 else None


# ----------------------------------------
# Last DB row
# ----------------------------------------
def db_get_last_spy_row():
    resp = (
        supabase.table(TABLE_SPY)
        .select("*")
        .order("candle_time", desc=True)
        .limit(1)
        .execute()
    )
    return resp.data[0] if resp.data else None


# ----------------------------------------
# Insert row
# ----------------------------------------
def db_insert_spy_row(payload):
    supabase.table(TABLE_SPY).insert(payload).execute()
    print("[DB] Inserted:", payload["candle_time"])


# ----------------------------------------
# Indicators
# ----------------------------------------
def compute_indicators(new_bar, prev_row):
    tp = (float(new_bar["high"]) + float(new_bar["low"]) + float(new_bar["close"])) / 3
    volume = float(new_bar.get("volume", 0))

    if prev_row:
        cumulative_pv_prev = float(prev_row["cumulative_pv"])
        cumulative_vol_prev = float(prev_row["cumulative_vol"])
        ema12_prev = float(prev_row["ema12"])
        ema26_prev = float(prev_row["ema26"])
        signal_prev = float(prev_row["macd_signal"])
    else:
        cumulative_pv_prev = 0
        cumulative_vol_prev = 0
        ema12_prev = float(new_bar["close"])
        ema26_prev = float(new_bar["close"])
        signal_prev = 0

    cumulative_pv = cumulative_pv_prev + tp * volume
    cumulative_vol = cumulative_vol_prev + volume
    vwap = cumulative_pv / cumulative_vol if cumulative_vol > 0 else tp

    close_price = float(new_bar["close"])
    alpha12 = 2 / 13
    alpha26 = 2 / 27
    alpha9 = 2 / 10

    ema12 = ema12_prev + alpha12 * (close_price - ema12_prev)
    ema26 = ema26_prev + alpha26 * (close_price - ema26_prev)
    macd = ema12 - ema26
    signal = signal_prev + alpha9 * (macd - signal_prev)
    hist = macd - signal

    return {
        "typical_price": tp,
        "cumulative_pv": cumulative_pv,
        "cumulative_vol": cumulative_vol,
        "vwap": vwap,
        "ema12": ema12,
        "ema26": ema26,
        "macd": macd,
        "macd_signal": signal,
        "macd_hist": hist,
    }


# ----------------------------------------
# MAIN CYCLE
# ----------------------------------------
def run_spy_cycle():
    new_bar = fetch_spy_5m()
    if not new_bar:
        print("No SPY data")
        return

    prev = db_get_last_spy_row()

    # prevent duplicates
    if prev and prev["candle_time"] == new_bar["date"]:
        print("Duplicate. Skipping.")
        return

    indicators = compute_indicators(new_bar, prev)

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

    db_insert_spy_row(payload)
    print("[Saifan] SPY cycle completed.")
