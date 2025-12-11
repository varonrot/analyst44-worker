# ======================================================
# Saifan Main Worker – Real-Time Background Loop
# Runs continuously every 20 seconds while market is open
# ======================================================

import time
import datetime
from saifan_01_spy_live_5min_quote_builder import run_cycle
from saifan_02_spy_5m_history_update import run_history_update   # 👈 נוסיף את זה

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
# Real-Time Loop (01 + 02)
# ------------------------------------------------------
def run_saifan_loop():
    print("=== Saifan Real-Time Worker Started ===")

    loop_counter = 0   # ← חדש

    while True:
        try:
            print(f"[Saifan] Heartbeat – loop alive... (#{loop_counter})")

            if is_us_market_open():
                # ------------------------------
                # 01 — Live QUOTE update
                # ------------------------------
                print("[Saifan] Market OPEN – updating SPY (LIVE)...")
                run_cycle()

                # ------------------------------
                # 02 — Official history update
                # רץ רק כל 15 לולאות = כל ~5 דקות
                # ------------------------------
                if loop_counter % 15 == 0:
                    print("[Saifan] Running OFFICIAL history update...")
                    run_history_update()
            else:
                print("[Saifan] Market CLOSED – sleeping...")

        except Exception as e:
            print("[Saifan] ERROR:", e)

        loop_counter += 1
        time.sleep(20)   # כל סיבוב 20 שניות


# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------
if __name__ == "__main__":
    run_saifan_loop()
