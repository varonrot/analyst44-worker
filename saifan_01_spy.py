import os
import requests
from datetime import datetime, timezone
import supabase

# -----------------------------
# ENV + CLIENT
# -----------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
FMP_API_KEY = os.environ["FMP_API_KEY"]

supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)
TABLE_NAME = "saifan_intraday_candles_spy_5m"


# -----------------------------
# Indicator functions
# -----------------------------
def calc_typical_price(o, h, l, c):
    return (h + l + c) / 3


def calc_ema(prev, price, period):
    if prev is None:
        return price
    k = 2 / (period + 1)
    return prev + k * (price - prev)


def calc_macd(ema12, ema26):
    if ema12 is None or ema26 is None:
        return None
    return ema12 - ema26


def calc_vwap(cpv, cvol):
    if cvol == 0:
        return None
    return cpv / cvol


# -----------------------------
# Fetch real 5m SPY bar
# -----------------------------
def fetch_last_5m_bar():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    print("[SPY] Fetching:", url)

    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]

    except Exception as e:
        print("[SPY ERROR] API request failed:", e)

    print("[SPY ERROR] Invalid data from FMP:", data)
    return None


# -----------------------------
# Get previous row (to continue calculations)
# -----------------------------
def get_last_row():
    res = (
        supabase_client.table(TABLE_NAME)
        .select("*")
        .order("candle_time", desc=True)
        .limit(1)
        .execute()
    )

    if res.data:
        return res.data[0]
    return None


# -----------------------------
# Full SPY 5m cycle
# -----------------------------
def run_spy_cycle():
    print("[SPY] START 5m CYCLE")

    bar = fetch_last_5m_bar()
    if not bar:
        print("[SPY ERROR] No bar to process.")
        return

    try:
        # Extract raw values
        o = float(bar["open"])
        h = float(bar["high"])
        l = float(bar["low"])
        c = float(bar["close"])
        v = float(bar["volume"])

        # Convert datetime
        dt = datetime.strptime(bar["date"], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        candle_time = dt.isoformat()

        # Previous row for indicators
        last = get_last_row()

        prev_cpv = last["cumulative_pv"] if last else 0
        prev_cvol = last["cumulative_vol"] if last else 0
        prev_ema12 = last["ema12"] if last else None
        prev_ema26 = last["ema26"] if last else None
        prev_signal = last["macd_signal"] if last else None

        # Indicators
        typical_price = calc_typical_price(o, h, l, c)

        cumulative_pv = prev_cpv + typical_price * v
        cumulative_vol = prev_cvol + v

        ema12 = calc_ema(prev_ema12, c, 12)
        ema26 = calc_ema(prev_ema26, c, 26)
        macd = calc_macd(ema12, ema26)

        if prev_signal is None:
            macd_signal = macd
        else:
            macd_signal = calc_ema(prev_signal, macd, 9)

        macd_hist = macd - macd_signal if macd and macd_signal else None

        vwap = calc_vwap(cumulative_pv, cumulative_vol)

        # Build payload
        payload = {
            "symbol": "SPY",
            "candle_time": candle_time,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "typical_price": typical_price,
            "cumulative_pv": cumulative_pv,
            "cumulative_vol": cumulative_vol,
            "ema12": ema12,
            "ema26": ema26,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
            "vwap": vwap,
        }

        # UPSERT into DB
        supabase_client.table(TABLE_NAME).upsert(
            payload,
            on_conflict=["symbol", "candle_time"]
        ).execute()

        print("[SPY] SAVED:", payload)

    except Exception as e:
        print("[SPY ERROR] FAILED:", e)
