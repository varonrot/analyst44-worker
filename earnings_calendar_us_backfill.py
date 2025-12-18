import os
from datetime import date
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TODAY = date.today().isoformat()

def step_5_backfill_missing_earnings():
    print("STEP 5: Backfilling symbols with missing latest earnings")

    # 1. Pull all symbols + last earnings date
    res = (
        supabase
        .table("analyst_financial_scores")
        .select("symbol, last_earnings_date")
        .execute()
    )

    if not res.data:
        print("No rows found in analyst_financial_scores")
        return

    symbols_added = 0

    for row in res.data:
        symbol = row["symbol"]
        last_earnings_date = row["last_earnings_date"]

        # 2. Skip if already updated today
        if last_earnings_date == TODAY:
            continue

        # 3. Check if symbol already exists in earnings_calendar_us
        exists = (
            supabase
            .table("earnings_calendar_us")
            .select("id")
            .eq("symbol", symbol)
            .limit(1)
            .execute()
        )

        if exists.data:
            continue

        # 4. Insert symbol for re-check
        supabase.table("earnings_calendar_us").insert({
            "symbol": symbol,
            "report_date": TODAY,
            "source": "backfill_step_5",
        }).execute()

        symbols_added += 1
        print(f"Added {symbol} to earnings_calendar_us")

    print(f"STEP 5 completed — {symbols_added} symbols added")


if __name__ == "__main__":
    step_5_backfill_missing_earnings()
