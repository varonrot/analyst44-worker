import time
from datetime import datetime, timezone
from saifan_01_spy import run_spy_cycle


# ---------------------------------------------------------
# Check if US stock market is open (regular hours)
# ---------------------------------------------------------
def is_us_market_open():
    now_utc = datetime.now(timezone.utc)

    # Monday=0 ... Sunday=6
    if now_utc.weekday() > 4:
        return False

    # Minutes since midnight
    total_minutes = now_utc.hour * 60 + now_utc.minute

    # Regular hours: 14:30–21:00 UTC (9:30–16:00 ET)
    market_open = 14 * 60 + 30
    market_close = 21 * 60

    return market_open <= total_minutes <= market_close


# ---------------------------------------------------------
# Main loop (runs every 5 minutes)
# ---------------------------------------------------------
def run_saifan_forever():
    print("=== Saifan Main Worker Started ===")

    while True:
        print("\n------------------------------------------")
        print(f"[Saifan] New cycle at {datetime.utcnow().isoformat()} UTC")

        if is_us_market_open():
            print("[Saifan] Market open - running SPY module...")
            run_spy_cycle()
        else:
            print("[Saifan] Market closed - skipping SPY processing.")

        print("[Saifan] Cycle completed. Sleeping 300 seconds...\n")

        time.sleep(300)  # 5 minutes


# ---------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------
if __name__ == "__main__":
    run_saifan_forever()
