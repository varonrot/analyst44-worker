# ======================================================
# saifan_main_worker.py
#
# Saifan Main Worker – Orchestrator
# Stage 1: Runs the SPY 5-minute LIVE quote builder
# Additional stages (02, 03...) will be triggered here later.
#
# This worker is executed every 5 minutes on Render.
# ======================================================

import datetime
from saifan_01_spy_live_5m import run_cycle


# ------------------------------------------------------
# Check if US markets are open
# ------------------------------------------------------
def is_us_market_open():
    """
    US Market hours:
        9:30–16:00 ET
        14:30–21:00 UTC
    Runs only Monday–Friday
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Monday=0 ... Sunday=6
    if now_utc.weekday() > 4:
        return False  # Weekend

    hour = now_utc.hour
    minute = now_utc.minute

    # Minutes from midnight UTC
    current_minutes = hour * 60 + minute

    market_open = 14 * 60 + 30   # 14:30 UTC
    market_close = 21 * 60       # 21:00 UTC

    return market_open <= current_minutes <= market_close


# ------------------------------------------------------
# Main Orchestrator (Stage 1 only)
# ------------------------------------------------------
def run_saifan():
    print("=== Saifan Main Worker Started ===")

    if not is_us_market_open():
        print("[Saifan] Market closed – skipping this 5-min cycle.")
        return

    print("[Saifan] Market OPEN – running Stage 1: SPY LIVE 5m")
    run_cycle()

    print("=== Saifan Stage 1 cycle complete ===")


# ------------------------------------------------------
# Manual test
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan()
