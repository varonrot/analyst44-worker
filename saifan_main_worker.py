# ======================================================
# Saifan Main Worker – Real-Time Background Loop
# Runs continuously every 20 seconds while market is open
# ======================================================

import time
import datetime
from saifan_01_spy_live_5min_quote_builder import run_cycle

# ------------------------------------------------------
# Check if US markets are open
# ------------------------------------------------------
def is_us_market_open():
    """
    US Market hours:
        9:30–16:00 ET
        14:30–21:00 UTC
    Runs only Monday–Friday.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Monday = 0 ... Sunday = 6
    if now_utc.weekday() > 4:
        return False  # Weekend

    hour = now_utc.hour
    minute = now_utc.minute

    # Minutes from midnight UTC
    current_minutes = hour * 60 + minute

    market_open = 14 * 60 + 30   # 14:30 UTC (09:30 ET)
    market_close = 21 * 60       # 21:00 UTC (16:00 ET)

    return market_open <= current_minutes <= market_close


# ------------------------------------------------------
# Continuous Loop – real-time engine
# ------------------------------------------------------
def run_saifan_loop():
    print("=== Saifan Real-Time Worker Started ===")

    while True:
        if is_us_market_open():
            print("[Saifan] Market OPEN – running Stage 1: SPY LIVE 5m quote builder")
            try:
                run_cycle()
            except Exception as e:
                print("[Saifan] ERROR:", e)
        else:
            print("[Saifan] Market closed – waiting...")

        # Real-time refresh interval (change if you want)
        time.sleep(20)  # every 20 seconds


# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan_loop()
