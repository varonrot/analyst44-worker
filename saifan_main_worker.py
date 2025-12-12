import time
import datetime

from saifan_00_reset_daily import run_daily_reset
from saifan_01_spy_live_5min_quote_builder import run_cycle
from saifan_02_spy_5m_history_update import run_history_update


def is_us_market_open():
    now = datetime.datetime.utcnow()
    hour = now.hour
    minute = now.minute
    total_minutes = hour * 60 + minute
    return (14 * 60 + 30) <= total_minutes <= (21 * 60)


def run_saifan_loop():
    print("=== Saifan Main Worker Started ===")

    last_history_run = 0
    last_reset_day = None  # Reset runs once per day

    while True:
        try:
            print("[Saifan] Heartbeat...")

            # ---------------------------------------------------
            # DAILY RESET — runs once per calendar day
            # ---------------------------------------------------
            today = datetime.date.today()
            if last_reset_day != today:
                print("[Saifan] Running DAILY RESET...")
                run_daily_reset()
                last_reset_day = today
                print("[Saifan] DAILY RESET completed")

            # ---------------------------------------------------
            # MARKET LOGIC
            # ---------------------------------------------------
            if is_us_market_open():

                # Live 5-minute update
                run_cycle()

                # History updater every 5 minutes
                now = time.time()
                if now - last_history_run >= 300:
                    print("[Saifan] Running official history updater...")
                    run_history_update()
                    last_history_run = now

            else:
                print("[Saifan] Market closed")

        except Exception as e:
            print("[Saifan ERROR]", e)

        time.sleep(20)


if __name__ == "__main__":
    run_saifan_loop()
