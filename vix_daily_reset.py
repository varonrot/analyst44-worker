import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing Supabase environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def run():
    print("Resetting vix_daily table...")
    supabase.table("vix_daily").delete().neq("trade_date", "1900-01-01").execute()
    print("vix_daily table reset completed")

if __name__ == "__main__":
    run()
