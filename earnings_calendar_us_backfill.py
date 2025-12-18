import os
from datetime import date, datetime
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)


def backfill_missing_symbols():
    today = date.today().isoformat()
    log("STEP 5: Backfilling symbols missing earnings report")

    # 1. Symbols analyzed today
    scores = (
        supabase
        .table("analyst_financial_scores")
        .select("symbol")
        .eq("analysis_date", today)
        .execute()
        .data
        or []
    )

    score_symbols = {r["symbol"] for r in scores if r.get("symbol")}

    # 2. Symbols already in earnings calendar today
    calendar = (
        supabase
        .table("earnings_calendar_us")
        .select("symbol")
        .eq("report_date", today)
        .execute()
        .data
        or []
    )

    calendar_symbols = {r["symbol"] for r in calendar if r.get("symbol")}

    missing = sorted(score_symbols - calendar_symbols)

    if not missing:
        log("No missing symbols found")
        return

    rows = []
    for symbol in missing:
        log(f"Inserted missing symbol: {symbol}")
        rows.append({
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

    supabase.table("earnings_calendar_us").insert(rows).execute()
    log(f"Inserted {len(rows)} missing symbols successfully")


if __name__ == "__main__":
    backfill_missing_symbols()
