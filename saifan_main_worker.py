# Saifan Main Worker – Real-Time Background Loop
# Runs continuously every 20 seconds while market is open

import time
import datetime

from saifan_01_spy_live_5min_quote_builder import run_cycle
from saifan_02_spy_5m_history_update import run_history_update


# ------------------------------------------------------------
# Check if US markets are open (UTC time)
# ------------------------------------------------------------
def is_us_market_open() -> bool:
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Monday = 0 ... Sunday = 6
    if now_utc.weekday() > 4:
        # Weekend
        return False

    hour = now_utc.hour
    minute = now_utc.minute
    current_minutes = hour * 60 + minute

    # US market hours in UTC: 14:30–21:00
    return (14 * 60 + 30) <= current_minutes <= (21 * 60)


# ------------------------------------------------------------
# Real-Time Loop (live + history)
# ------------------------------------------------------------
def run_saifan_loop():
    print("=== Saifan Real-Time Worker Started ===")

    # last official history run (epoch seconds)
    last_history_run: float = 0.0

    while True:
        try:
            print("[Saifan] Heartbeat - loop alive...")

            if is_us_market_open():

                # 01 – LIVE quote update (every loop, ~20s)
                print("[Saifan] Market OPEN - updating SPY (LIVE)...")
                run_cycle()

                # 02 – OFFICIAL history update (every 5 minutes)
                now = time.time()
                if last_history_run == 0.0 or (now - last_history_run) >= 300:
                    print("[Saifan] Running OFFICIAL history update...")
                    run_history_update()
                    last_history_run = now

            else:
                print("[Saifan] Market CLOSED - sleeping...")

        except Exception as e:
            print("[Saifan] ERROR:", e)

        # wait 20 seconds between loops
        time.sleep(20)


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    run_saifan_loop()
