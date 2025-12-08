# ======================================================
# saifan_main_worker.py
# Saifan Main Worker – orchestrator
# Runs every 5 minutes, but only during US market hours.
# Currently runs only SPY module (saifan_01_spy.py)
# ======================================================

import datetime
import pytz
from saifan_01_spy import run_spy_cycle


# ------------------------------------------------------
# Check if US markets are open right now
# ------------------------------------------------------
def is_us_market_open():
    """
    Market hours: 9:30–16:00 EST
    In UTC: 14:30–21:00
    Monday–Friday only
    """

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Monday=0 ... Sunday=6
    if now_utc.weekday() > 4:
        return False  # Weekend

    hour = now_utc.hour
    minute = now_utc.minute

    # Convert to minutes from midnight UTC
    current_minutes = hour * 60 + minute

    open_minutes = 14 * 60 + 30   # 14:30 UTC
    close_minutes = 21 * 60       # 21:00 UTC

    return open_minutes <= current_minutes <= close_minutes


# ------------------------------------------------------
# Main orchestrator
# ------------------------------------------------------
def run_saifan():
    print("=== Saifan Main Worker Started ===")

    if not is_us_market_open():
        print("[Saifan] Market closed – skipping cycle.")
        return

    print("[Saifan] Market open – running SPY module...")
    run_spy_cycle()

    print("=== Saifan cycle completed ===")


# ------------------------------------------------------
# Manual test
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan()
