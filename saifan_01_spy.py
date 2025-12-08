# ======================================================
# saifan_01_spy.py
# Complete SPY pipeline:
# Fetch SPY → Load last row → Compute Indicators → Insert
# ======================================================

import os
import requests
from supabase import create_client
from datetime import datetime, timezone

# -----------------------------
# Environment Variables
# -----------------------------
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_SPY = "saifan_intraday_candles_spy_5m"


# ======================================================
# FETCH SPY (FMP)
# ======================================================

def fetch_spy_5m():
    """
    Fetch latest SPY 5-minute candle from FMP.
    """
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]  # latest bar
        print("[SPY Fetch] Empty SPY response.")
        return None
    except Exception as e:
        print("[SPY Fetch] Error:", e)
        return None


# ======================================================
# DB Operations
# ======================================================

def db_get_last_spy_row():
    """
    Fetch the most recent SPY row from DB.
    """
    try:
        resp = (
            supabase.table(TABLE_SPY)
            .select("*")
            .order("candle_time", desc=True)
            .limit(1)
            .execute()
        )
        return resp.data[0] if resp.data else None
    except Exception as e:
        print("[DB] Error fetching last SPY row:", e)
        return None


def db_insert_spy_row(payload: dict):
    """
    Insert full SPY row with indicators.
    """
    try:
        supabase.table(TABLE_SPY).insert(payload).execute()
        print("[DB] Inserted SPY row:", payload["candle_time"])
        return True
    except Exception as e:
        print("[DB] Insert failed:", e)
        return False


# ======================================================
# Indicator Calculations
# ======================================================

def compute_indicators(new_bar, prev_row):
    """
    Computes: typical price, cumulative_pv, cumulative_vol, vwap,
    ema12, ema26, macd, signal (ema9), hist.
    """

    # ---------------- Typical Price ----------------
    tp = (float(new_bar["high"]) + float(new_bar["low"]) + float(new_bar["close"])) / 3

    volume = float(new_bar.get("volume", 0))

    # ---------------- Previous Values ----------------
    if prev_row:
        cumulative_pv_prev = float(prev_row.get("cumulative_pv", 0) or 0)
        cumulative_vol_prev = float(prev_row.get("cumulative_vol", 0) or 0)
        ema12_prev = float(prev_row.get("ema12", 0) or 0)
        ema26_prev = float(prev_row.get("ema26", 0) or 0)
        signal_prev = float(prev_row.get("macd_signal", 0) or 0)
    else:
        cumulative_pv_prev = 0
        cumulative_vol_prev = 0
        ema12_prev = float(new_bar["close"])
        ema26_prev = float(new_bar["close"])
        signal_prev = 0

    # ---------------- Cumulative VWAP ----------------
    cumulative_pv = cumulative_pv_prev + (tp * volume)
    cumulative_vol = cumulative_vol_prev + volume
    vwap = cumulative_pv / cumulative_vol if cumulative_vol > 0 else tp

    close_price = float(new_bar["close"])

    # ---------------- EMA12 ----------------
    alpha12 = 2 / (12 + 1)
    ema12 = ema12_prev + alpha12 * (close_price - ema12_prev)

    # ---------------- EMA26 ----------------
    alpha26 = 2 / (26 + 1)
    ema26 = ema26_prev + alpha26 * (close_price - ema26_prev)

    # ---------------- MACD ----------------
    macd = ema12 - ema26

    # ---------------- Signal Line (EMA9) ----------------
    alpha9 = 2 / (9 + 1)
    signal = signal_prev + alpha9 * (macd - signal_prev)

    # ---------------- Histogram ----------------
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


# ======================================================
# MAIN SPY PROCESSOR
# ======================================================

def run_spy_cycle():
    """
    Main SPY update function:
    Fetch → Compute → Insert
    """
    new_bar = fetch_spy_5m()
    if not new_bar:
        print("[SPY] No new bar fetched.")
        return

    # Fetch last row for indicator context
    prev_row = db_get_last_spy_row()

    # Duplicate check
    if prev_row and prev_row["candle_time"] == new_bar["date"]:
        print("[SPY] Duplicate bar, skipping.")
        return

    # Compute indicators
    indicators = compute_indicators(new_bar, prev_row)

    # Prepare payload for DB
    payload = {
        "symbol": "SPY",
        "candle_time": new_bar["date"],
        "open": float(new_bar["open"]),
        "high": float(new_bar["high"]),
        "low": float(new_bar["low"]),
        "close": float(new_bar["close"]),
        "volume": float(new_bar.get("volume", 0)),

        **indicators,  # add all computed fields
    }

    # Insert into DB
