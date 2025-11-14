import os
import time
import uuid
import requests

from datetime import datetime, UTC
from dotenv import load_dotenv
from supabase import create_client, Client

# ==========================================
# Load environment variables
# ==========================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not FMP_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / FMP_API_KEY in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Tables
EARNINGS_CALENDAR_TABLE = "earnings_calendar_us"
INCOME_TABLE = "income_statements_last"
JOBS_MONITOR_TABLE = "jobs_monitor"

# Job metadata
JOB_NAME = "income_statements_last_sync"
JOB_GROUP = "financials"

# FMP rate-limit (300 calls/minute ≈ 5 calls/second)
FMP_SLEEP_SECONDS = 0.2


# ------------------------------------------
# 0. Clear income_statements_last (daily reset)
# ------------------------------------------
def clear_income_table():
    """
    מוחק את כל השורות ב-income_statements_last.
    אנחנו רוצים שהטבלה תייצג רק את מי שמפרסם היום.
    """
    print("Clearing table income_statements_last ...")
    # PostgREST דורש פילטר כלשהו, לכן נשתמש ב-neq על עמודה שיש בה always value
    (
        supabase
        .table(INCOME_TABLE)
        .delete()
        .neq("symbol", "")  # מוחק את כל השורות (מתאים כי symbol תמיד לא ריק)
        .execute()
    )
    print("Table income_statements_last cleared.")


# ------------------------------------------
# 1. Load symbols from earnings_calendar_us
# ------------------------------------------
def load_symbols_from_calendar() -> list[str]:
    """
    מחזיר רשימת סימבולים ייחודיים מתוך earnings_calendar_us.
    לפי מה שהגדרת – הטבלה כבר מכילה רק את מי שמפרסם היום.
    """
    print("Loading symbols from Supabase table earnings_calendar_us ...")

    resp = (
        supabase
        .table(EARNINGS_CALENDAR_TABLE)
        .select("symbol")
        .range(0, 9999)
        .execute()
    )

    rows = resp.data or []
    symbols_set = set()

    for row in rows:
        sym = row.get("symbol")
        if not sym:
            continue

        # לפי ההגיון הקודם שלך: רק סימבולים בלי נקודה ופחות מ-5 תווים
        if "." in sym:
            continue
        if len(sym) >= 5:
            continue

        symbols_set.add(sym)

    symbols = sorted(symbols_set)
    print(f"Total unique symbols from earnings_calendar_us after filtering: {len(symbols)}")
    return symbols


# ------------------------------------------
# 2. Fetch last income statement for symbol
# ------------------------------------------
def fetch_last_income_statement(symbol: str) -> dict | None:
    """
    מביא את הדוח האחרון בלבד עבור סימבול.
    מחזיר dict או None אם אין נתונים.
    """
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}"
    params = {
        "apikey": FMP_API_KEY,
        "limit": 1,
        "period": "quarter",  # <<< חדש – מבקש רק דוחות רבעוניים
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data:
            print(f"[WARN] No income statement for {symbol}")
            return None
        return data[0]
    except Exception as e:
        print(f"[ERROR] Failed to fetch income statement for {symbol}: {e}")
        return None

def is_quarterly_statement(stmt: dict) -> bool:
    period = (stmt.get("period") or "").upper()
    return period in {"Q1", "Q2", "Q3", "Q4"}

# ------------------------------------------
# 2b. Filter – only USD currency
# ------------------------------------------
def is_usd_statement(stmt: dict) -> bool:
    """
    מחזיר True רק אם הדוח במטבע USD.
    בלי סינון לפי תאריך – רק לפי מטבע.
    """
    return stmt.get("reportedCurrency") == "USD"


# ------------------------------------------
# 3. Upsert into income_statements_last
# ------------------------------------------
def upsert_income_statement(symbol: str, stmt: dict) -> bool:
    """
    מבצע upsert לרשומה אחת בטבלה income_statements_last.
    מחזיר True אם הצליח, False אחרת.
    """
    row = {
        "symbol": symbol,
        "date": stmt.get("date"),
        "reported_currency": stmt.get("reportedCurrency"),
        "revenue": stmt.get("revenue"),
        "gross_profit": stmt.get("grossProfit"),
        "operating_income": stmt.get("operatingIncome"),
        "net_income": stmt.get("netIncome"),
        "eps": stmt.get("eps"),
        "diluted_eps": stmt.get("epsdiluted") or stmt.get("epsDiluted"),
        "operating_expenses": stmt.get("operatingExpenses"),
        "income_before_tax": stmt.get("incomeBeforeTax"),
        "income_tax_expense": stmt.get("incomeTaxExpense"),
        "net_income_ratio": stmt.get("netIncomeRatio"),
        "raw_json": stmt,
        "updated_at": datetime.now(UTC).isoformat(),
    }

    try:
        resp = (
            supabase.table(INCOME_TABLE)
            .upsert(row, on_conflict="symbol")
            .execute()
        )
        status = getattr(resp, "status_code", None)
        if status not in (200, 201, None):  # None בחלק מהגרסאות של supabase-py
            print(f"[ERROR] Upsert failed for {symbol}, status={status}")
            return False
        print(f"[OK] {symbol} upserted (date={row['date']})")
        return True
    except Exception as e:
        print(f"[ERROR] Exception during upsert for {symbol}: {e}")
        return False


# ------------------------------------------
# 4. Jobs monitor helpers
# ------------------------------------------
def start_job_monitor_run() -> tuple[int, datetime, dict]:
    """
    מכניס רשומה חדשה ל-jobs_monitor ומחזיר:
    (job_id, started_at_dt, job_row_dict)
    """
    run_id = str(uuid.uuid4())
    started_at_dt = datetime.now(UTC)
    started_at_iso = started_at_dt.isoformat()

    row = {
        "run_id": run_id,
        "job_name": JOB_NAME,
        "job_group": JOB_GROUP,
        "run_source": os.getenv("JOB_RUN_SOURCE", "local"),
        "trigger_type": os.getenv("JOB_TRIGGER_TYPE", "manual"),
        "server": os.getenv("JOB_SERVER", "local"),
        "status": "running",
        "started_at": started_at_iso,
        "finished_at": None,
        "duration_ms": None,
        "rows_fetched": 0,
        "rows_inserted": 0,
        "rows_failed": 0,
        "error_message": None,
    }

    resp = supabase.table(JOBS_MONITOR_TABLE).insert(row).execute()
    data = resp.data or []
    if not data:
        raise RuntimeError("Failed to insert into jobs_monitor; no data returned")

    job_id = data[0]["id"]
    print(f"Started jobs_monitor run id={job_id}, run_id={run_id}")
    return job_id, started_at_dt, row


def finish_job_monitor_run(
    job_id: int,
    started_at_dt: datetime,
    job_row: dict,
    status: str,
    error_message: str | None = None,
):
    """
    מעדכן את רשומת ה-job בסיום ריצה.
    """
    finished_at_dt = datetime.now(UTC)
    finished_at_iso = finished_at_dt.isoformat()
    duration_ms = int((finished_at_dt - started_at_dt).total_seconds() * 1000)

    update_fields = {
        "status": status,
        "finished_at": finished_at_iso,
        "duration_ms": duration_ms,
        "rows_fetched": job_row.get("rows_fetched", 0),
        "rows_inserted": job_row.get("rows_inserted", 0),
        "rows_failed": job_row.get("rows_failed", 0),
        "error_message": error_message,
    }

    supabase.table(JOBS_MONITOR_TABLE).update(update_fields).eq("id", job_id).execute()
    print(
        f"Finished jobs_monitor run id={job_id}, status={status}, "
        f"rows_inserted={update_fields['rows_inserted']}, "
        f"rows_failed={update_fields['rows_failed']}"
    )


# ------------------------------------------
# 5. Main
# ------------------------------------------
def main():
    job_id = None
    started_at_dt = None
    job_row = None

    try:
        # התחלת רישום ב-jobs_monitor
        job_id, started_at_dt, job_row = start_job_monitor_run()

        # שלב 0: ניקוי טבלת היעד (טבלה יומית שמתעדכנת מחדש)
        clear_income_table()

        # טעינת יקום הסימבולים מהקלנדר ב-Supabase (היום בלבד לפי מה שבנית)
        symbols = load_symbols_from_calendar()
        job_row["rows_fetched"] = len(symbols)

        # ריצה על כל הסימבולים
        for idx, symbol in enumerate(symbols, start=1):
            print(f"\n[{idx}/{len(symbols)}] Processing {symbol} ...")

            stmt = fetch_last_income_statement(symbol)
            if stmt is None:
                job_row["rows_failed"] += 1
                time.sleep(FMP_SLEEP_SECONDS)
                continue

            # 🟦 הוספה כאן — בדיקה שהדוח רבעוני בלבד
            if not is_quarterly_statement(stmt):
                # לא רבעוני → מדלגים (לא נספר כ-failed)
                time.sleep(FMP_SLEEP_SECONDS)
                continue

            # 🟩 בדיקה קיימת — רק USD
            if not is_usd_statement(stmt):
                # מדלגים – לא נספר כ-failed
                time.sleep(FMP_SLEEP_SECONDS)
                continue

            # סינון – רק דוחות במטבע USD
            if not is_usd_statement(stmt):
                # מדלגים – לא נספר כ-failed, פשוט לא רלוונטי
                time.sleep(FMP_SLEEP_SECONDS)
                continue

            ok = upsert_income_statement(symbol, stmt)
            if ok:
                job_row["rows_inserted"] += 1
            else:
                job_row["rows_failed"] += 1

            # כיבוד Rate Limit של FMP
            time.sleep(FMP_SLEEP_SECONDS)

        # סיום מוצלח
        finish_job_monitor_run(
            job_id=job_id,
            started_at_dt=started_at_dt,
            job_row=job_row,
            status="success",
            error_message=None,
        )

    except Exception as e:
        print(f"[FATAL] Job failed: {e}")
        if job_id is not None and started_at_dt is not None and job_row is not None:
            finish_job_monitor_run(
                job_id=job_id,
                started_at_dt=started_at_dt,
                job_row=job_row,
                status="error",
                error_message=str(e),
            )
        else:
            print("[FATAL] Failed before jobs_monitor row was created")


if __name__ == "__main__":
    main()
