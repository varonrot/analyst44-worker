import time
import datetime
from saifan_01_spy_live_5min_quote_builder import run_cycle
from saifan_02_spy_5m_history_update import run_history_update

def is_us_market_open():
    now = datetime.datetime.utcnow()
    hour = now.hour
    minute = now.minute
    current = hour * 60 + minute
    return (14 * 60 + 30) <= current <= (21 * 60)

def run_saifan_loop():
    print("=== Saifan Main Worker Started ===")

    last_history_run = 0

    while True:
        try:
            print("[Saifan] Heartbeat...")

            if is_us_market_open():

                # LIVE UPDATE
                run_cycle()

                # HISTORY UPDATE every 5 minutes
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
