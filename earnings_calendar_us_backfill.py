import os
from datetime import date, datetime, timedelta
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)


def backfill_missing_symbols():
    today_dt = date.today()
    today = today_dt.isoformat()
    three_weeks_ago = today_dt - timedelta(days=21)

    log("STEP 5: Backfilling symbols missing earnings report")

    # 1. Symbols analyzed today (כולל last_earnings_date)
    scores = (
        supabase
        .table("analyst_financial_scores")
        .select("symbol, last_earnings_date")
        .eq("analysis_date", today)
        .execute()
        .data
        or []
    )

    eligible_symbols = set()

    for r in scores:
        symbol = r.get("symbol")
        last_date = r.get("last_earnings_date")

        if not symbol:
            continue

        # ✔️ אין דוח קודם
        if not last_date:
            eligible_symbols.add(symbol)
            continue

        try:
            last_dt = date.fromisoformat(last_date)
        except Exception:
            eligible_symbols.add(symbol)
            continue

       # ✔️ דוח ישן מ־3 שבועות
       if last_dt < three_weeks_ago:
           eligible_symbols.add(symbol)


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

    # 🔑 ההבדל הקריטי כאן
    missing = sorted(eligible_symbols - calendar_symbols)

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
