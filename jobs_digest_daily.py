#!/usr/bin/env python3
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from supabase import create_client

# --------------------------------------------------
# Config
# --------------------------------------------------
JOB_NAME = "earnings_calendar_daily"
LOCAL_TZ = ZoneInfo("Asia/Jerusalem")
HTML_OUTPUT_FILE = "jobs_digest_daily.html"

TABLE_JOBS_MONITOR = "jobs_monitor"
TABLE_JOBS_DAILY = "jobs_monitor_daily"


# --------------------------------------------------
# Supabase client
# --------------------------------------------------
def get_supabase_client():
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not service_key:
        raise RuntimeError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env"
        )

    return create_client(url, service_key)


# --------------------------------------------------
# Date helpers
# --------------------------------------------------
def get_today_range_local_to_utc():
    """Return (start_utc, end_utc, date_local_str) for *today* in LOCAL_TZ.

    - start_utc / end_utc: boundaries in UTC for the local day
    - date_local_str: YYYY-MM-DD string for storing in jobs_monitor_daily
    """
    now_local = datetime.now(LOCAL_TZ)
    start_local = datetime(
        year=now_local.year,
        month=now_local.month,
        day=now_local.day,
        tzinfo=LOCAL_TZ,
    )
    end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    return start_utc, end_utc, start_local.date().isoformat()


# --------------------------------------------------
# Fetch jobs from jobs_monitor
# --------------------------------------------------
def _parse_ts(value):
    """Parse timestamp coming from Supabase (string or datetime)."""
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        # Supabase usually returns ISO strings, sometimes with 'Z'
        s = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    else:
        return None

    # Ensure tz-aware (assume UTC if missing)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_jobs_for_today(supabase):
    """Fetch all runs from jobs_monitor that belong to today (LOCAL_TZ)."""
    start_utc, end_utc, date_local = get_today_range_local_to_utc()

    resp = (
        supabase
        .table(TABLE_JOBS_MONITOR)
        .select("*")
        .order("started_at", desc=False)
        .execute()
    )
    all_rows = resp.data or []

    rows_today = []
    for r in all_rows:
        started = _parse_ts(r.get("started_at"))
        if not started:
            continue
        if start_utc <= started < end_utc:
            rows_today.append(r)

    print("[jobs_digest_daily] date_local=", date_local,
          "runs_today=", len(rows_today))
    return rows_today, date_local



# --------------------------------------------------
# Stats
# --------------------------------------------------
def compute_stats(rows):
    runs = len(rows)
    successes = 0
    failures = 0

    durations_sec = []
    rows_processed_total = 0
    last_finished_at = None

    for r in rows:
        status = (r.get("status") or "").lower()
        if status == "success":
            successes += 1
        elif status == "failed":
            failures += 1

        # duration of single run
        dur = r.get("duration_sec")
        if isinstance(dur, (int, float)):
            durations_sec.append(float(dur))

        # rows fetched / processed
        rf = r.get("rows_fetched") or r.get("rows_processed")
        if isinstance(rf, (int, float)):
            rows_processed_total += int(rf)

        # last finish time
        finished_at = _parse_ts(r.get("finished_at"))
        if finished_at:
            if not last_finished_at or finished_at > last_finished_at:
                last_finished_at = finished_at

    if durations_sec:
        avg_duration_sec = sum(durations_sec) / len(durations_sec)
        durations_sorted = sorted(durations_sec)
        mid = len(durations_sorted) // 2
        if len(durations_sorted) % 2 == 1:
            median_duration_sec = durations_sorted[mid]
        else:
            median_duration_sec = (
                durations_sorted[mid - 1] + durations_sorted[mid]
            ) / 2
        max_duration_sec = max(durations_sec)
    else:
        avg_duration_sec = 0.0
        median_duration_sec = 0.0
        max_duration_sec = 0.0

    return {
        "runs": runs,
        "successes": successes,
        "failures": failures,
        "avg_duration_sec": avg_duration_sec,
        "median_duration_sec": median_duration_sec,
        "max_duration_sec": max_duration_sec,
        "rows_processed": rows_processed_total,
        "last_finished_at": last_finished_at,
    }


# --------------------------------------------------
# Write daily row to jobs_monitor_daily
# --------------------------------------------------
def upsert_daily_row(supabase, date_local, stats):
    """Delete+insert for (job_name, date_local) to avoid duplicate-key errors."""
    if stats["runs"] > 0:
        avg_duration_ms = int(stats["avg_duration_sec"] * 1000)
    else:
        avg_duration_ms = None

    # המרה ל-string כדי ש־Supabase יקבל ISO ולא datetime
    lf = stats["last_finished_at"]
    if isinstance(lf, datetime):
        lf = lf.isoformat()

    row = {
        "job_name": JOB_NAME,
        "date_local": date_local,
        "runs": stats["runs"],
        "success": stats["successes"],
        "failed": stats["failures"],
        "avg_duration_ms": avg_duration_ms,
        "rows_total": stats["rows_processed"],
        "last_finished_at": lf,   # כבר מומר ל־string או None
    }

    # קודם מוחקים את הרשומה של אותו job + תאריך (אם קיימת)
    supabase.table(TABLE_JOBS_DAILY) \
        .delete() \
        .eq("job_name", JOB_NAME) \
        .eq("date_local", date_local) \
        .execute()

    # ואז מכניסים מחדש
    supabase.table(TABLE_JOBS_DAILY) \
        .insert(row) \
        .execute()




# --------------------------------------------------
# HTML rendering
# --------------------------------------------------
def render_html(date_local, stats):
    def fmt_sec(x):
        return f"{x:.1f}s"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Jobs Digest — {date_local}</title>
  <style>
    body {{
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #0b1726;
      color: #e8f6ff;
      padding: 24px;
    }}
    .card {{
      max-width: 640px;
      margin: 0 auto;
      background: #121e2d;
      border-radius: 16px;
      padding: 24px 28px;
      box-shadow: 0 18px 40px rgba(0,0,0,0.55);
      border: 1px solid rgba(255,255,255,0.06);
    }}
    h1 {{
      font-size: 22px;
      margin: 0 0 12px;
    }}
    .subtitle {{
      font-size: 13px;
      color: #9ab3c4;
      margin-bottom: 18px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .metric {{
      background: rgba(3,20,40,0.9);
      border-radius: 12px;
      padding: 10px 12px;
      border: 1px solid rgba(193,214,255,0.08);
    }}
    .label {{
      font-size: 12px;
      color: #9ab3c4;
      margin-bottom: 4px;
    }}
    .value {{
      font-size: 17px;
      font-weight: 600;
    }}
    .pill-row {{
      margin-top: 6px;
      margin-bottom: 14px;
    }}
    .pill {{
      display: inline-block;
      padding: 3px 9px;
      border-radius: 999px;
      font-size: 11px;
      margin-right: 6px;
    }}
    .pill-success {{
      background: rgba(34,197,94,0.12);
      color: #4ade80;
      border: 1px solid rgba(34,197,94,0.45);
    }}
    .pill-fail {{
      background: rgba(239,68,68,0.12);
      color: #fb7185;
      border: 1px solid rgba(239,68,68,0.45);
    }}
    .footer {{
      margin-top: 18px;
      font-size: 11px;
      color: #71879a;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Job monitor — {JOB_NAME}</h1>
    <div class="subtitle">
      Summary for <strong>{date_local}</strong>
    </div>

    <div class="pill-row">
      <span class="pill pill-success">✔ Success: {stats['successes']}</span>
      <span class="pill pill-fail">✖ Failed: {stats['failures']}</span>
    </div>

    <div class="grid">
      <div class="metric">
        <div class="label">Runs today</div>
        <div class="value">{stats['runs']}</div>
      </div>
      <div class="metric">
        <div class="label">Rows processed</div>
        <div class="value">{stats['rows_processed']}</div>
      </div>
      <div class="metric">
        <div class="label">Avg duration</div>
        <div class="value">{fmt_sec(stats['avg_duration_sec'])}</div>
      </div>
      <div class="metric">
        <div class="label">Max duration</div>
        <div class="value">{fmt_sec(stats['max_duration_sec'])}</div>
      </div>
      <div class="metric">
        <div class="label">Median duration</div>
        <div class="value">{fmt_sec(stats['median_duration_sec'])}</div>
      </div>
    </div>

    <div class="footer">
      This file was generated by <code>jobs_digest_daily.py</code>
      and can be attached to an email as HTML.
    </div>
  </div>
</body>
</html>
"""


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    supabase = get_supabase_client()

    rows, date_local = fetch_jobs_for_today(supabase)
    stats = compute_stats(rows)

    # 1) update daily summary table
    upsert_daily_row(supabase, date_local, stats)

    # 2) write HTML locally
    html = render_html(date_local, stats)
    with open(HTML_OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print("✓ Digest generated:")
    print(
        {
            "runs": stats["runs"],
            "successes": stats["successes"],
            "failures": stats["failures"],
            "avg_duration_sec": stats["avg_duration_sec"],
            "median_duration_sec": stats["median_duration_sec"],
            "max_duration_sec": stats["max_duration_sec"],
            "rows_processed": stats["rows_processed"],
            "html_file": os.path.abspath(HTML_OUTPUT_FILE),
        }
    )


if __name__ == "__main__":
    main()
