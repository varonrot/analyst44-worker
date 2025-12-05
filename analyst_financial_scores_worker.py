# analyst_financial_scores_worker.py
#
# Loop over analyst_input_1_symbols,
# fetch last 2 reports from analyst_input_financial_statements,
# send them to GPT with analyst_financial_scoring_prompt.txt,
# and store the scores into analyst_financial_scores.

import os
import json
from datetime import date

from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client, Client

from openai import OpenAI


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

BASE_DIR = Path(__file__).resolve().parent
PROMPT_PATH = BASE_DIR / "analyst_financial_scoring_prompt.txt"


def load_system_prompt() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def get_symbols() -> list[str]:
    """Fetch all symbols from earnings_calendar_us."""
    res = supabase.table("earnings_calendar_us").select("symbol").execute()
    data = res.data or []
    return [row["symbol"] for row in data if row.get("symbol")]


def get_two_latest_reports(symbol: str):
    """
    Fetch the two latest reports for a symbol from analyst_input_financial_statements.
    Returns (latest_report, previous_report) as dicts or (None, None) if not enough data.
    """
    res = (
        supabase.table("analyst_input_financial_statements")
        .select("*")
        .eq("symbol", symbol)
        .order("report_date", desc=True)
        .limit(2)
        .execute()
    )
    rows = res.data or []
    if len(rows) < 2:
        return None, None

    latest = rows[0]
    previous = rows[1]

    return latest, previous


def prepare_payload(latest: dict, previous: dict) -> dict:
    """
    Build the payload sent to GPT:
    {
      "latest_report": { ... all fields + analysis_date ... },
      "previous_report": { ... all fields (without analysis_date/D11 requirement) ... }
    }
    We also inject analysis_date (today) into latest_report for the model to echo.
    """
    # Ensure we don't mutate original dicts from Supabase client
    latest_report = dict(latest)
    previous_report = dict(previous)

    # Inject analysis_date into latest_report (if not already present)
    if "analysis_date" not in latest_report or latest_report["analysis_date"] is None:
        latest_report["analysis_date"] = date.today().isoformat()

    payload = {
        "latest_report": latest_report,
        "previous_report": previous_report,
    }
    return payload


def call_gpt(system_prompt: str, payload: dict) -> dict | None:
    """
    Call GPT with the system prompt and the two-report payload.
    Expects JSON in the exact format we defined in the system prompt.
    Returns parsed dict or None on failure.
    """
    user_content = json.dumps(payload, default=str)

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0.1,
        )
    except Exception as e:
        print(f"  Error calling GPT: {e}")
        return None

    try:
        content = resp.choices[0].message.content
    except Exception as e:
        print(f"  Error reading GPT response: {e}")
        return None

    if not content:
        print("  Empty GPT response content")
        return None

    try:
        data = json.loads(content)
    except Exception as e:
        print(f"  Failed to parse GPT JSON: {e}")
        print("  Raw content:", content)
        return None

    return data


def insert_score_row(gpt_data: dict):
    """
    Insert one row into analyst_financial_scores from the GPT JSON output.
    Expects all required fields to be present according to the spec.
    """
    # Defensive defaults
    symbol = gpt_data.get("symbol")
    last_earnings_date = gpt_data.get("last_earnings_date")
    analysis_date = gpt_data.get("analysis_date")

    echo_price = gpt_data.get("echo_price")

    total_score = gpt_data.get("total_score")
    profitability = gpt_data.get("profitability")
    growth = gpt_data.get("growth")
    financial_strength = gpt_data.get("financial_strength")

    target_range = gpt_data.get("target_range") or {}
    target_range_low = target_range.get("low")
    target_range_high = target_range.get("high")

    # NEW FIELD
    direction = gpt_data.get("direction")

    swing_forecast = gpt_data.get("swing_forecast_weeks_2_3")
    summary_30_words = gpt_data.get("summary_30_words")
    volatility_flag = gpt_data.get("volatility_flag")

    comparison_trend = gpt_data.get("comparison_trend")

    row = {
        "symbol": symbol,
        "last_earnings_date": last_earnings_date,
        "analysis_date": analysis_date,
        "echo_price": echo_price,
        "total_score": total_score,
        "profitability": profitability,
        "growth": growth,
        "financial_strength": financial_strength,
        "target_range_low": target_range_low,
        "target_range_high": target_range_high,
        "direction": direction,                     # <<< NEW LINE
        "swing_forecast_weeks_2_3": swing_forecast,
        "summary_30_words": summary_30_words,
        "volatility_flag": volatility_flag,
        "comparison_trend": comparison_trend,
    }

    try:
        supabase.table("analyst_financial_scores").insert(row).execute()
    except Exception as e:
        print(f"  Error inserting into analyst_financial_scores for {symbol}: {e}")


def process_symbol(symbol: str, system_prompt: str):
    print(f"Processing scores for {symbol} ...")

    latest, previous = get_two_latest_reports(symbol)
    if latest is None or previous is None:
        print(f"  Not enough reports for {symbol}, skipping.")
        return

    payload = prepare_payload(latest, previous)
    gpt_data = call_gpt(system_prompt, payload)
    if not gpt_data:
        print(f"  No valid GPT data for {symbol}, skipping.")
        return

    insert_score_row(gpt_data)
    print(f"  Scores saved for {symbol}.")


def run_worker():
    print("Starting analyst_financial_scores_worker...")

    system_prompt = load_system_prompt()
    symbols = get_symbols()

    print(f"Found {len(symbols)} symbols to process.")

    for symbol in symbols:
        try:
            process_symbol(symbol, system_prompt)
        except Exception as e:
            print(f"Unexpected error processing {symbol}: {e}")

    print("Done.")


if __name__ == "__main__":
    run_worker()
