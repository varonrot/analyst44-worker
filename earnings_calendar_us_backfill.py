import os
from supabase import create_client
from datetime import date

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def backfill_missing_symbols():
    print("STEP 5: Backfilling symbols missing from earnings_calendar_us")

    # 1. Symbols with scores but no calendar row
    rows = (
        supabase
        .table("analyst_financial_scores")
        .select("symbol, last_earnings_date")
        .execute()
    )

    if not rows.data:
        print("No financial scores found")
        return

    symbols = {r["symbol"]: r["last_earnings_date"] for r in rows.data}

    existing = (
        supabase
        .table("earnings_calendar_us")
        .select("symbol")
        .execute()
    )

    existing_symbols = {r["symbol"] for r in (existing.data or [])}

    to_insert = []

    for symbol, last_earnings_date in symbols.items():
        if symbol in existing_symbols:
            continue

        to_insert.append({
            "symbol": symbol,
            "report_date": date.today().isoformat(),
            "time": "unknown",
            "company_name": None,
            "market_cap": None,
            "fiscal_quarter_ending": None,
            "consensus_eps": None,
        })

        print(f"Inserted missing symbol: {symbol}")

    if to_insert:
        supabase.table("earnings_calendar_us").insert(to_insert).execute()
        print(f"Inserted {len(to_insert)} symbols")
    else:
        print("No missing symbols to insert")


if __name__ == "__main__":
    backfill_missing_symbols()
