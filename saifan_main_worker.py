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
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Monday = 0 ... Sunday = 6
    if now_utc.weekday() > 4:
        return False

    hour = now_utc.hour
    minute = now_utc.minute

    current_minutes = hour * 60 + minute

    # Market hours UTC: 14:30–21:00
    return (14 * 60 + 30) <= current_minutes <= (21 * 60)


# ------------------------------------------------------
# Real-Time Loop
# ------------------------------------------------------
def run_saifan_loop():
    print("=== Saifan Real-Time Worker Started ===")

    while True:
        try:
            # 👇 הלוג החדש — יופיע כל 20 שניות
            print("[Saifan] Heartbeat – loop is alive...")

            if is_us_market_open():
                print("[Saifan] Market OPEN – updating SPY...")
                run_cycle()
            else:
                print("[Saifan] Market CLOSED – sleeping...")

        except Exception as e:
            print("[Saifan] ERROR:", e)

        time.sleep(20)



# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan_loop()
