import time
import datetime

from saifan_01_spy_live_5min_quote_builder import run_cycle
from saifan_02_spy_5m_history_update import run_history_update
from saifan_03_vix_live_5min_quote_builder import run_vix_cycle
from saifan_04_vix_5m_history_update import run_vix_history_update
from saifan_00_reset_daily import run_daily_reset


def is_us_market_open():
    now = datetime.datetime.utcnow()
    minutes = now.hour * 60 + now.minute
    return (14 * 60 + 30) <= minutes <= (21 * 60)


def run_saifan_loop():
    print("=== Saifan Main Worker Started ===")

    last_spy_history = 0
    last_vix_history = 0
    did_reset_today = False

    while True:
        try:
            now = datetime.datetime.utcnow()
            print("[Saifan] Heartbeat")

            # DAILY RESET before market opens
            if now.hour < 14 and not did_reset_today:
                print("[Saifan] Daily reset running...")
                run_daily_reset()
                did_reset_today = True

            if now.hour >= 14:
                did_reset_today = False

            # MARKET OPEN
            if is_us_market_open():

                # Live SPY
                run_cycle()

                # Live VIX
                run_vix_cycle()

                # Official SPY history update every 5 minutes
                t = time.time()
                if t - last_spy_history >= 300:
                    print("[Saifan] Running SPY history update...")
                    run_history_update()
                    last_spy_history = t

                # Official VIX history update every 5 minutes
                if t - last_vix_history >= 300:
                    print("[Saifan] Running VIX history update...")
                    run_vix_history_update()
                    last_vix_history = t

            else:
                print("[Saifan] Market closed")

        except Exception as e:
            print("[Saifan ERROR]", e)

        time.sleep(20)


if __name__ == "__main__":
    run_saifan_loop()
