import datetime
from supabase import create_client
import os

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

TABLES_TO_RESET = [
    "saifan_intraday_candles_spy_5m",
    "saifan_intraday_vix_5m",
]

def run_daily_reset():
    now = datetime.datetime.utcnow()

    # Reset only before market opens - before 14:30 UTC
    if now.hour > 13:
        print("[RESET] Too late to reset today")
        return

    print("[RESET] Clearing old daily tables...")

    for table in TABLES_TO_RESET:
        try:
            supabase.table(table).delete().neq("id", 0).execute()
            print(f"[RESET] Cleared table: {table}")
        except Exception as e:
            print(f"[RESET ERROR] {table}", e)
