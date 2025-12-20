# spy_daily_bars_reset.py

from supabase import create_client
import os

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

print("Resetting spy_daily_bars table...")

supabase.table("spy_daily_bars").delete().neq("symbol", "").execute()

print("spy_daily_bars reset completed")
