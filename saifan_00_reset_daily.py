from supabase import create_client
import datetime
import os

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

TABLE = "saifan_intraday_candles_spy_5m"

def run_reset_daily():
    # UTC date of today
    today = datetime.datetime.utcnow().date()

    print("[Saifan RESET] Clearing table for new trading day...")

    supabase.table(TABLE).delete().neq("candle_time", None).execute()

    print("[Saifan RESET] Done. Table is empty.")
