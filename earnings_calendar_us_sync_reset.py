import requests
import json
import os
from datetime import datetime
from supabase import create_client, Client

# --------------------------------------------------------
# CONFIG
# --------------------------------------------------------
NASDAQ_URL = "https://api.nasdaq.com/api/calendar/earnings?date={date}"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") 
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# --------------------------------------------------------
# Helpers – Time field normalization
# --------------------------------------------------------
def normalize_report_time(t: str) -> str:
    if not t:
        return ""
    lt = t.lower()
    if "before" in lt or "bmo" in lt:
        return "Before Market Open"
    if "after" in lt or "amc" in lt:
        return "After Market Close"
    return t.strip()


# --------------------------------------------------------
# Fetch from Nasdaq
# --------------------------------------------------------
def fetch_raw_from_nasdaq(date: str):
    url = NASDAQ_URL.format(date=date)
    print(f"\nFetching earnings from NASDAQ for {date}")
    print(f"URL: {url}\n")

    try:
        res = requests.get(url, headers=HEADERS)
        if res.status_code != 200:
            print("❌ Error:", res.status_code)
            return None
        return res.json()
    except Exception as e:
        print("❌ Exception:", e)
        return None


# --------------------------------------------------------
# Extract only required fields
# --------------------------------------------------------
def parse_row(r: dict, report_date: str):
    raw_time = (
        r.get("epsReportTime")
        or r.get("time")
        or r.get("Time")
        or ""
    )

    return {
        "report_date": report_date,
        "time": normalize_report_time(str(raw_time)),
        "symbol": str(r.get("symbol", "")).strip(),
        "company_name": str(r.get("name", "")).strip(),
        "market_cap": str(r.get("marketCap", "")).strip(),
        "fiscal_quarter_ending": str(r.get("fiscalQuarterEnding", "")).strip(),
        "consensus_eps_forecast": str(r.get("epsForecast", "")).strip(),
        "num_of_ests": r.get("noOfEsts", r.get("numOfEsts", "")),
        "last_year_report_date": str(r.get("lastYearRptDate", "")).strip(),
        "last_year_eps": str(r.get("lastYearEPS", "")).strip(),
    }


# --------------------------------------------------------
# PUSH rows to Supabase
# --------------------------------------------------------
def push_rows_to_supabase(rows):
    print("\nInserting rows to Supabase...")

    for row in rows:
        supabase.table("earnings_calendar_us").insert(row).execute()

    print(f"Inserted {len(rows)} rows into earnings_calendar_us")


# --------------------------------------------------------
# RESET today's data
# --------------------------------------------------------
def reset_today(report_date: str):
    print(f"\nResetting existing rows for {report_date}...")
    supabase.table("earnings_calendar_us").delete().eq("report_date", report_date).execute()
    print("Done.\n")


# --------------------------------------------------------
# MAIN
# --------------------------------------------------------
def main():
    today = datetime.today().strftime("%Y-%m-%d")

    raw = fetch_raw_from_nasdaq(today)

    if (
        raw is None
        or "data" not in raw
        or raw["data"] is None
        or "rows" not in raw["data"]
        or not raw["data"]["rows"]
    ):
        print("❌ No rows to insert.")
        return

    nasdaq_rows = raw["data"]["rows"]

    # RESET table for today
    reset_today(today)

    # Parse all rows
    parsed_rows = [parse_row(r, today) for r in nasdaq_rows]

    # Insert all rows
    push_rows_to_supabase(parsed_rows)

    print("\nAll done!\n")


if __name__ == "__main__":
    main()
