import time
import datetime

from saifan_00_reset_daily import run_daily_reset

# SPY modules
from saifan_01_spy_live_5min_quote_builder import run_cycle as run_cycle_spy
from saifan_02_spy_5m_history_update import run_history_update as run_spy_history

# VIX modules
from saifan_03_vix_live_5min_quote_builder import run_cycle_vix
from saifan_04_vix_5m_history_update import run_vix_history_update


# ------------------------------------------------------
# Market open check (UTC 14:30–21:00)
# ------------------------------------------------------
def is_us_market_open():
    now = datetime.datetime.utcnow()
    hour = now.hour
    minute = now.minute
    total_minutes = hour * 60 + minute
    return (14 * 60 + 30) <= total_minutes <= (21 * 60)


# ------------------------------------------------------
# Main loop – runs 24/7
# ------------------------------------------------------
def run_saifan_loop():
    print("=== Saifan Main Worker Started ===")

    last_history_run = 0
    last_reset_day = None

    while True:
        try:
            print("[Saifan] Heartbeat...")

            # ---------------------------------------------------
            # DAILY RESET (once per calendar day)
            # ---------------------------------------------------
            today = datetime.date.today()
            if last_reset_day != today:
                print("[Saifan] Running DAILY RESET...")
                run_daily_reset()
                last_reset_day = today
                print("[Saifan] DAILY RESET completed")

            # ---------------------------------------------------
            # MARKET OPEN LOGIC
            # ---------------------------------------------------
            if is_us_market_open():

                # -------------------
                # LIVE UPDATES
                # -------------------
                run_cycle_spy()
                run_cycle_vix()

                # -------------------
                # HISTORY UPDATES (every 5 minutes)
                # -------------------
                now = time.time()
                if now - last_history_run >= 300:
                    print("[Saifan] Running SPY history updater...")
                    run_spy_history()

                    print("[Saifan] Running VIX history updater...")
                    run_vix_history_update()

                    last_history_run = now

            else:
                print("[Saifan] Market closed")

        except Exception as e:
            print("[Saifan ERROR]", e)

        time.sleep(20)


# ------------------------------------------------------
# Entry Point
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan_loop()
