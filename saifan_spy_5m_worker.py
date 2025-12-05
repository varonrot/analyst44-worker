import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= CONFIG =========
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_SPY = "saifan_intraday_candles_spy_5m"
TABLE_VIX = "saifan_intraday_vix_5m"
TABLE_STOCKS = "saifan_intraday_stocks_5m"
TABLE_STOCK_LIST = "saifan_stock_list"

# ========= HELPERS =========

def fetch_symbol_5m(symbol):
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}?apikey={FMP_API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        print(f"Fetch error for {symbol}:", e)
        return None


def is_today_utc(candle_time_str: str) -> bool:
    ct = datetime.fromisoformat(candle_time_str)
    today = datetime.now(timezone.utc).date()
    return ct.date() == today

def candle_exists_simple(table, candle_time: str) -> bool:
    resp = (
        supabase.table(table)
        .select("id")
        .eq("candle_time", candle_time)
        .execute()
    )
    return len(resp.data) > 0

def candle_exists(table, symbol, candle_time: str) -> bool:
    resp = (
        supabase.table(table)
        .select("id")
        .eq("symbol", symbol)
        .eq("candle_time", candle_time)
        .execute()
    )
    return len(resp.data) > 0

def insert_spy_with_indicators(bar):
    prev_pv, prev_vol, prev_ema12, prev_ema26, prev_macd, prev_signal = get_previous_spy_state()

    typical = (float(bar["high"]) + float(bar["low"]) + float(bar["close"])) / 3.0
    volume = float(bar.get("volume") or 0)

    new_pv = prev_pv + typical * volume
    new_vol = prev_vol + volume
    vwap = new_pv / new_vol if new_vol > 0 else typical

    close_price = float(bar["close"])

    k12 = 2/13
    k26 = 2/27
    k9  = 2/10

    ema12 = (close_price - prev_ema12) * k12 + prev_ema12
    ema26 = (close_price - prev_ema26) * k26 + prev_ema26

    macd = ema12 - ema26
    macd_signal = (macd - prev_signal) * k9 + prev_signal
    macd_hist = macd - macd_signal

    data = {
        "candle_time": bar["date"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": close_price,
        "volume": volume,
        "cumulative_pv": new_pv,
        "cumulative_vol": new_vol,
        "vwap": vwap,
        "ema12": ema12,
        "ema26": ema26,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "symbol": "SPY",
    }

    supabase.table(TABLE_SPY).insert(data).execute()
    print("Inserted SPY indicators:", data)

def insert_vix(bar):
    data = {
        "candle_time": bar["date"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"])
    }
    supabase.table(TABLE_VIX).insert(data).execute()
    print("Inserted VIX:", data)

def get_previous_spy_state():
    resp = (
        supabase.table(TABLE_SPY)
        .select("cumulative_pv, cumulative_vol, ema12, ema26, macd, macd_signal, candle_time")
        .order("candle_time", desc=True)
        .limit(1)
        .execute()
    )

    if resp.data:
        row = resp.data[0]
        return (
            float(row.get("cumulative_pv") or 0),
            float(row.get("cumulative_vol") or 0),
            float(row.get("ema12") or 0),
            float(row.get("ema26") or 0),
            float(row.get("macd") or 0),
            float(row.get("macd_signal") or 0),
            row.get("candle_time")
        )
    return 0, 0, 0, 0, 0, 0, None

def reset_spy_daily_state():
    # delete all rows from today's date backwards
    supabase.table(TABLE_SPY).delete().neq("id", -1).execute()
    print("SPY daily reset completed.")


def insert_spy_with_indicators(bar):
    prev_pv, prev_vol, prev_ema12, prev_ema26, prev_macd, prev_signal, prev_time = get_previous_spy_state()

    candle_time = bar["date"]
    candle_date = candle_time.split("T")[0]

    if prev_time:
        prev_date = prev_time.split("T")[0]
        if candle_date != prev_date:
            prev_pv = prev_vol = 0
            prev_ema12 = prev_ema26 = 0
            prev_macd = prev_signal = 0
            print("New day detected — resetting SPY accumulators.")

    open_price = float(bar["open"])
    high = float(bar["high"])
    low = float(bar["low"])
    close_price = float(bar["close"])
    volume = float(bar.get("volume") or 0)

    typical = (high + low + close_price) / 3
    new_pv = prev_pv + (typical * volume)
    new_vol = prev_vol + volume
    vwap = new_pv / new_vol if new_vol > 0 else typical

    k12 = 2 / 13
    k26 = 2 / 27
    k9 = 2 / 10

    ema12 = (close_price - prev_ema12) * k12 + prev_ema12
    ema26 = (close_price - prev_ema26) * k26 + prev_ema26

    macd = ema12 - ema26
    macd_signal = (macd - prev_signal) * k9 + prev_signal
    macd_hist = macd - macd_signal

    data = {
        "symbol": "SPY",
        "candle_time": candle_time,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close_price,
        "volume": volume,
        "cumulative_pv": new_pv,
        "cumulative_vol": new_vol,
        "vwap": vwap,
        "ema12": ema12,
        "ema26": ema26,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist
    }

    supabase.table(TABLE_SPY).insert(data).execute()
    print("Inserted SPY with indicators.")


# ========= NEW: STOCK LIST + INSERT =========

def fetch_stock_list():
    """Returns list of stock symbols from saifan_stock_list where active=true."""
    resp = supabase.table(TABLE_STOCK_LIST).select("symbol").eq("active", True).execute()
    return [row["symbol"] for row in resp.data]

def get_previous_vwap_state(symbol):
    resp = (
        supabase.table(TABLE_STOCKS)
        .select("cumulative_pv, cumulative_vol")
        .eq("symbol", symbol)
        .order("candle_time", desc=True)
        .limit(1)
        .execute()
    )

    if resp.data:
        row = resp.data[0]
        return float(row["cumulative_pv"]), float(row["cumulative_vol"])
    return 0.0, 0.0

def get_previous_macd_state(symbol):
    resp = (
        supabase.table(TABLE_STOCKS)
        .select("ema12, ema26, macd, macd_signal")
        .eq("symbol", symbol)
        .order("candle_time", desc=True)
        .limit(1)
        .execute()
    )

    if resp.data:
        row = resp.data[0]
        return (
            float(row.get("ema12") or 0),
            float(row.get("ema26") or 0),
            float(row.get("macd") or 0),
            float(row.get("macd_signal") or 0)
        )
    return 0.0, 0.0, 0.0, 0.0

def insert_stock_candle(symbol, bar):
    # === VWAP previous ===
    prev_pv, prev_vol = get_previous_vwap_state(symbol)

    typical = (float(bar["high"]) + float(bar["low"]) + float(bar["close"])) / 3.0
    volume = float(bar.get("volume") or 0)

    new_pv = prev_pv + (typical * volume)
    new_vol = prev_vol + volume
    vwap = new_pv / new_vol if new_vol > 0 else typical

    # === MACD previous state ===
    prev_ema12, prev_ema26, prev_macd, prev_signal = get_previous_macd_state(symbol)

    close_price = float(bar["close"])

    # EMA multipliers
    k12 = 2 / 13
    k26 = 2 / 27
    k9  = 2 / 10

    # Incremental EMA
    ema12 = (close_price - prev_ema12) * k12 + prev_ema12
    ema26 = (close_price - prev_ema26) * k26 + prev_ema26

    # MACD + Signal
    macd = ema12 - ema26
    macd_signal = (macd - prev_signal) * k9 + prev_signal
    macd_hist = macd - macd_signal

    data = {
        "symbol": symbol,
        "candle_time": bar["date"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": close_price,
        "volume": volume,
        "cumulative_pv": new_pv,
        "cumulative_vol": new_vol,
        "vwap": vwap,
        "ema12": ema12,
        "ema26": ema26,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist
    }

    supabase.table(TABLE_STOCKS).insert(data).execute()
    print("Inserted STOCK:", symbol, "| VWAP:", vwap, "| MACD:", macd)

def process_stock(symbol):
    """Fetch + insert one 5m candle for symbol."""
    bar = fetch_symbol_5m(symbol)
    if not bar:
        print("No data for", symbol)
        return

    candle_time = bar["date"]

    if not is_today_utc(candle_time):
        print("Skipping", symbol, ": candle not today")
        return

    if candle_exists(TABLE_STOCKS, symbol, candle_time):
        print(f"Skipping {symbol}: duplicate candle {candle_time}.")
        return

    insert_stock_candle(symbol, bar)

# ========= MAIN =========

def process_symbol(symbol, table, insert_func):
    bar = fetch_symbol_5m(symbol)
    if not bar:
        print(f"No data for {symbol}.")
        return

    candle_time = bar["date"]

    if not is_today_utc(candle_time):
        print(f"Skipping {symbol}: candle not from today.")
        return

    if candle_exists_simple(table, candle_time):
        print(f"Skipping {symbol}: duplicate candle {candle_time}.")
        return

    insert_func(bar)


def main():
    print("Running combined SPY, VIX & 150 STOCKS worker...")

    # === DAILY RESET FOR SPY ===
    spy_latest = fetch_symbol_5m("SPY")
    if spy_latest:
        new_date = spy_latest["date"].split("T")[0]

        resp = supabase.table(TABLE_SPY).select("candle_time").order("candle_time", desc=True).limit(1).execute()
        if resp.data:
            last_date = resp.data[0]["candle_time"].split("T")[0]

            if new_date != last_date:
                reset_spy_daily_state()
                print("New trading day detected → SPY table RESET.")

    process_symbol("SPY", TABLE_SPY, insert_spy_with_indicators)
    process_symbol("^VIX", TABLE_VIX, insert_vix)

    # === NEW: PROCESS 150 STOCKS ===
    stock_list = fetch_stock_list()
    print("Processing", len(stock_list), "stocks...")

    for sym in stock_list:
        process_stock(sym)


if __name__ == "__main__":
    main()
