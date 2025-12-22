import os
from datetime import datetime
from supabase import create_client, Client

APP_VERSION = 20251222_1018  # YYYYMMDD_HHMM

# ==================================================
# CONFIG
# ==================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing Supabase environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ==================================================
# LOGGING
# ==================================================

def log(message: str):
    ts = datetime.utcnow().isoformat()
    print(f"[NEWS-REVALIDATION] {ts} | {message}")

log(f"🚀 NEWS REVALIDATION VERSION LOADED: {APP_VERSION}")


def get_symbols_from_fmp_news():
    log("Fetching symbols from fmp_news")

    res = supabase.table("fmp_news") \
        .select("symbol") \
        .execute()

    if not res.data:
        log("No rows found in fmp_news")
        return []

    symbols = sorted({row["symbol"] for row in res.data if row.get("symbol")})

    log(f"Found {len(symbols)} unique symbols in fmp_news")
    return symbols


# ==================================================
# STAGE 2 – COLLECT NEWS FOR SYMBOL
# ==================================================

def collect_news_for_symbol(symbol: str):
    log(f"Fetching news for symbol: {symbol}")

    response = (
        supabase
        .table("fmp_news")
        .select("site, title, body")
        .eq("symbol", symbol)
        .order("published_at", desc=True)
        .execute()
    )

    if not response.data:
        log(f"No news found for {symbol}")
        return []

    news_items = [
        {
            "site": row.get("site"),
            "title": row.get("title"),
            "body": row.get("body"),
        }
        for row in response.data
        if row.get("title") and row.get("body")
    ]

    log(f"Fetched {len(news_items)} news items for {symbol}")
    return news_items


# ==================================================
# STAGE 3 – COLLECT BASELINE FROM analyst_financial_scores
# ==================================================

def collect_baseline_for_symbol(symbol: str):
    log(f"Fetching baseline from analyst_financial_scores for {symbol}")

    response = (
        supabase
        .table("analyst_financial_scores")
        .select(
            "symbol, analysis_date, last_earnings_date, total_score, "
            "profitability, growth, financial_strength, "
            "target_range_low, target_range_high, "
            "swing_forecast_weeks_2_3, volatility_flag, summary_30_words"
        )
        .eq("symbol", symbol)
        .order("analysis_date", desc=True)
        .limit(1)
        .execute()
    )

    if not response.data:
        log(f"No baseline found for {symbol}")
        return None

    baseline = response.data[0]

    log(
        f"Baseline loaded for {symbol} | "
        f"date={baseline['analysis_date']} | "
        f"score={baseline['total_score']}"
    )

    return baseline

# ==================================================
# STAGE 4 – SEND BASELINE + NEWS TO AI (CLEAN & STABLE)
# ==================================================

import json
import os
import re
from openai import OpenAI

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def run_revalidation_ai(symbol: str, baseline: dict, news_items: list):
    log(f"Running AI revalidation for {symbol}")

    # --- load prompt template ---
    with open("A44_Fundamental_News_Reconcile.txt", "r") as f:
        prompt_template = f.read()

    # --- build news block ---
    news_block = ""
    for i, n in enumerate(news_items, start=1):
        news_block += f"""
[{i}]
Source: {n.get('site')}
Title: {n.get('title')}
Body: {n.get('body')}
"""

    # --- build prompt ---
    prompt = prompt_template.format(
        symbol=symbol,
        base_score=baseline["total_score"],
        news_block=news_block
    )

    log(f"Prompt built for {symbol} (news={len(news_items)})")

    # --- call OpenAI ---
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "You are a financial analyst AI. Return ONLY valid JSON."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.2
    )

    raw_text = response.choices[0].message.content.strip()
    log(f"AI response received for {symbol}")

    # --- parse JSON strictly ---
    try:
        ai_result = json.loads(raw_text)
    except Exception as e:
        log(f"ERROR parsing AI JSON for {symbol}: {e}")
        log(raw_text)
        return None

    # --- normalize keys (CRITICAL FIX) ---
    def normalize_key(k: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_]", "", k)

    ai_result = {
        normalize_key(k): v
        for k, v in ai_result.items()
    }

    log(f"🔥 STRIP FIX ACTIVE | VERSION={APP_VERSION} | SYMBOL={symbol}")

    # --- validate required keys ---
    required_keys = {
        "updated_total_score",
        "bias_label",
        "summary_30_words"
    }

    missing = required_keys - ai_result.keys()
    if missing:
        log(f"❌ Missing keys after normalization for {symbol}: {missing}")
        log(f"AI RESULT KEYS ({symbol}): {list(ai_result.keys())}")
        log(f"AI RESULT CONTENT ({symbol}): {ai_result}")
        return None

    # --- cast + sanity checks ---
    try:
        ai_result["updated_total_score"] = int(ai_result["updated_total_score"])
    except Exception:
        log(f"ERROR: updated_total_score not castable to int for {symbol}")
        log(f"VALUE: {ai_result.get('updated_total_score')}")
        return None

    if not (0 <= ai_result["updated_total_score"] <= 100):
        log(f"ERROR: updated_total_score out of range for {symbol}")
        return None

    log(
        f"AI REVALIDATION OK ({symbol}) → "
        f"score={ai_result['updated_total_score']} | bias={ai_result['bias_label']}"
    )

    return ai_result

# ==================================================
# STAGE 5 – SAVE AI RESULT TO news_analyst_revalidation
# ==================================================

from datetime import datetime

def update_news_analyst_revalidation(symbol: str, analysis_date, ai_result: dict):
    log(f"Persisting AI revalidation result for {symbol}")

    payload = {
        "symbol": symbol,
        "analysis_date": analysis_date,

        "updated_total_score": ai_result["updated_total_score"],
        "bias_label": ai_result["bias_label"],
        "summary_30_words": ai_result["summary_30_words"],

        "updated_at": datetime.utcnow().isoformat()
    }

    supabase.table("news_analyst_revalidation") \
        .upsert(payload, on_conflict="symbol,analysis_date") \
        .execute()

    log(f"news_analyst_revalidation updated for {symbol}")



# ==================================================
# ORCHESTRATION
# ==================================================

def run_for_symbol(symbol: str):
    log(f"Starting processing for symbol: {symbol}")

    news_items = collect_news_for_symbol(symbol)
    if not news_items:
        log(f"No news found for {symbol} – skipping")
        return

    baseline = collect_baseline_for_symbol(symbol)
    if not baseline or not baseline.get("total_score"):
        log(f"No baseline results for {symbol} – skipping")
        return

    # ---- STAGE 4 ----
    ai_result = run_revalidation_ai(symbol, baseline, news_items)
    if not ai_result:
        log(f"No AI result for {symbol} – skipping DB update")
        return

    # ---- STAGE 5 (NEW) ----
    update_news_analyst_revalidation(
        symbol=symbol,
        analysis_date=baseline["analysis_date"],
        ai_result=ai_result
    )

    log(f"Finished processing for symbol: {symbol}")


def main():
    log("News Fundamental Revalidation Runner started")

    symbols = get_symbols_from_fmp_news()
    if not symbols:
        log("No symbols found in fmp_news – exiting")
        return

    for symbol in symbols:
        try:
            run_for_symbol(symbol)
        except Exception as e:
            log(f"ERROR processing {symbol}: {e}")

    log("News Fundamental Revalidation Runner finished")


if __name__ == "__main__":
    main()
