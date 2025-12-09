import time
from datetime import datetime, timezone
from saifan_01_spy import run_spy_cycle

def run_saifan_forever():
    print("=== Saifan Main Worker Started (FORCE MODE) ===")

    while True:
        print("\n------------------------------------------")
        print(f"[Saifan] New forced cycle at {datetime.utcnow().isoformat()} UTC")

        # ALWAYS RUN — no market hours check
        try:
            run_spy_cycle()
        except Exception as e:
            print(f"[Saifan ERROR] {e}")

        print("[Saifan] Forced cycle completed. Sleeping 300 seconds...\n")
        time.sleep(300)

if __name__ == "__main__":
    run_saifan_forever()
