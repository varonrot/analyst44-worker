import os, requests, uuid, json, time
from supabase import create_client
from datetime import datetime
from dotenv import load_dotenv

# ---------- env ----------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

TABLE_NAME = "earnings_calendar_us"
JOBS_TABLE = "jobs_monitor"
JOB_NAME   = "earnings_calendar_daily"

# ---------- helpers ----------
def log_job_start(run_id):
    supabase.table(JOBS_TABLE).insert({
        "run_id": run_id,
        "job_name": JOB_NAME,
        "status": "running",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "run_source": "render-cron",
    }).execute()

def log_job_finish(run_id, rows_fetched, rows_inserted, started_ts):
    supabase.table(JOBS_TABLE).update({
        "status": "success",
        "finished_at": datetime.utcnow().isoformat() + "Z",
        "duration_ms": int((time.perf_counter() - started_ts) * 1000),
        "rows_fetched": rows_fetched,
        "rows_inserted": rows_inserted,
    }).eq("run_id", run_id).execute()

def log_job_fail(run_id, err_msg, started_ts):
    supabase.table(JOBS_TABLE).update({
        "status": "failed",
        "finished_at": datetime.utcnow().isoformat() + "Z",
        "duration_ms": int((time.perf_counter() - started_ts) * 1000),
        "error_message": err_msg[:900],
    }).eq("run_id", run_id).execute()

# ---------- FMP ----------
def fetch_calendar_from_fmp():
    base_url = "https://financialmodelingprep.com/api/v3/earning_calendar"
    today = datetime.today().strftime("%Y-%m-%d")
    url = f"{base_url}?from={today}&to={today}&apikey={FMP_API_KEY}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def delete_table_data():
    supabase.table(TABLE_NAME).delete().neq("symbol", "").execute()

def insert_new_rows(data):
    chunk, total = 200, 0
    for i in range(0, len(data), chunk):
        supabase.table(TABLE_NAME).insert(data[i:i+chunk]).execute()
        total += len(data[i:i+chunk])
    return total

# ---------- main ----------
def main():
    run_id = str(uuid.uuid4())
    t0 = time.perf_counter()
    log_job_start(run_id)

    try:
        calendar = fetch_calendar_from_fmp()
        rows = [{
            "symbol": x.get("symbol"),
            "date": x.get("date"),
            "eps": x.get("eps"),
            "eps_estimated": x.get("epsEstimated"),
            "revenue": x.get("revenue"),
            "time": x.get("time"),
        } for x in calendar]

        # US filter: no '.' and len<=4
        rows = [r for r in rows if r["symbol"] and "." not in r["symbol"] and len(r["symbol"]) <= 4]

        delete_table_data()
        inserted = insert_new_rows(rows)

        log_job_finish(run_id, rows_fetched=len(calendar), rows_inserted=inserted, started_ts=t0)

    except Exception as e:
        log_job_fail(run_id, repr(e), started_ts=t0)
        raise

if __name__ == "__main__":
    main()
