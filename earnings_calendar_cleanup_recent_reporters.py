import os
from datetime import date, datetime, timedelta
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)


def cleanup_recent_reporters():
    today_dt = date.today()
    three_weeks_ago = today_dt - timedelta(days=21)

    log("CLEANUP: Removing symbols that reported earnings in last 3 weeks")

    # 1. Symbols that reported recently
    recent = (
        supabase
        .table("analyst_financial_scores")
        .select("symbol, last_earnings_date")
        .gte("last_earnings_date", three_weeks_ago.isoformat())
        .execute()
        .data
        or []
    )

    recent_symbols = {r["symbol"] for r in recent if r.get("symbol")}

    if not recent_symbols:
        log("No recent reporters found – nothing to clean")
        return

    log(f"Found {len(recent_symbols)} symbols to remove from earnings_calendar_us")

    # 2. Delete from earnings calendar
    for symbol in sorted(recent_symbols):
        supabase.table("earnings_calendar_us").delete().eq("symbol", symbol).execute()
        log(f"Removed symbol from earnings_calendar_us: {symbol}")

    log("Cleanup completed successfully")


if __name__ == "__main__":
    cleanup_recent_reporters()
