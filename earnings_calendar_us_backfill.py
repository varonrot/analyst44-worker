import os
from datetime import date, datetime
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)


def backfill_missing_earnings():
    today = date.today().isoformat()
    log(f"STEP 5: Backfilling missing earnings for {today}")

    # 1. Get all symbols analyzed today
    scores_resp = (
        supabase
        .table("analyst_financial_scores")
        .select("symbol")
        .eq("analysis_date", today)
        .execute()
    )

    if not scores_resp.data:
        log("No symbols found in analyst_financial_scores for today.")
        return

    score_symbols = {row["symbol"] for row in scores_resp.data if row.get("symbol")}
    log(f"Found {len(score_symbols)} symbols with scores today")

    # 2. Get all symbols already in earnings_calendar_us for today
    calendar_resp = (
        supabase
        .table("earnings_calendar_us")
        .select("symbol")
        .eq("report_date", today)
        .execute()
    )

    calendar_symbols = {
        row["symbol"] for row in (calendar_resp.data or []) if row.get("symbol")
    }

    # 3. Symbols missing from calendar
    missing_symbols = sorted(score_symbols - calendar_symbols)

    if not missing_symbols:
        log("No missing symbols to backfill.")
        return

    log(f"Backfilling {len(missing_symbols)} missing symbols")

    rows_to_insert = []
    for symbol in missing_symbols:
        rows_to_insert.append({
            "symbol": symbol,
            "report_date": today,
            "time": "missing earnings – added by system",
            "company_name": None,
            "market_cap": None,
            "fiscal_quarter_ending": None,
            "consensus_eps_forecast": None,
            "num_of_ests": None,
            "last_year_report_date": None,
            "last_year_eps": None,
        })

    # 4. Insert rows
    insert_resp = (
        supabase
        .table("earnings_calendar_us")
        .insert(rows_to_insert)
        .execute()
    )

    log(f"Inserted {len(rows_to_insert)} rows into earnings_calendar_us")
    log("STEP 5 completed successfully")


if __name__ == "__main__":
    backfill_missing_earnings()
