import time
import datetime

# SPY imports
from saifan_01_spy_live_5min_quote_builder import run_cycle as run_spy_live
from saifan_02_spy_5m_history_update import run_history_update as run_spy_history

# VIX imports (according to your files!)
from saifan_03_vix_live_5min_quote_builder import run_vix_live
from saifan_04_vix_5m_history_update import run_vix_history

# Daily reset
from saifan_00_reset_daily import run_daily_reset


# ------------------------------------------------------
# Market open check (UTC 14:30–21:00)
# ------------------------------------------------------
def is_us_market_open():
    now = datetime.datetime.utcnow()
    minutes = now.hour * 60 + now.minute
    return (14 * 60 + 30) <= minutes <= (21 * 60)


# ------------------------------------------------------
# Main worker loop
# ------------------------------------------------------
def run_saifan_loop():
    print("=== Saifan Main Worker Started ===")

    last_spy_history = 0
    last_vix_history = 0
    did_reset_today = False

    while True:
        try:
            now = datetime.datetime.utcnow()
            print("[Saifan] Heartbeat")

            # -------------------------
            # DAILY RESET (once per day)
            # -------------------------
            if now.hour < 14 and not did_reset_today:
                print("[Saifan] Running daily reset...")
                run_daily_reset()
                did_reset_today = True

            if now.hour >= 14:
                did_reset_today = False

            # -------------------------
            # MARKET OPEN LOGIC
            # -------------------------
            if is_us_market_open():

                # -------- LIVE SPY --------
                run_spy_live()

                # -------- LIVE VIX --------
                run_vix_live()

                # Time now
                now_ts = time.time()

                # -------- SPY HISTORY (5 min) --------
                if now_ts - last_spy_history >= 300:
                    print("[Saifan] SPY history update...")
                    run_spy_history()
                    last_spy_history = now_ts

                # -------- VIX HISTORY (5 min) --------
                if now_ts - last_vix_history >= 300:
                    print("[Saifan] VIX history update...")
                    run_vix_history()
                    last_vix_history = now_ts

            else:
                print("[Saifan] Market closed")

        except Exception as e:
            print("[Saifan ERROR]", e)

        time.sleep(20)


# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan_loop()
