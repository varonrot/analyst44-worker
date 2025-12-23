import os
import json
from datetime import datetime, timezone
from supabase import create_client, Client
from openai import OpenAI
import re

APP_VERSION = "2025-12-22_26"

# ==================================================
# CONFIG
# ==================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise Exception("Missing environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPT_PATH = os.path.join(BASE_DIR, "A44_Fundamental_News_Reconcile.txt")

# ==================================================
# LOGGING
# ==================================================

def log(msg: str):
    ts = datetime.utcnow().isoformat()
    print(f"[A44-AI-STEP-2.6] {ts} | {msg}", flush=True)

log(f"🚀 AI Revalidation Runner Loaded | VERSION={APP_VERSION}")

# ==================================================
# LOAD PROMPT
# ==================================================

with open(PROMPT_PATH, "r") as f:
    PROMPT_TEMPLATE = f.read()

# ==================================================
# FETCH INPUT ROWS
# ==================================================

def fetch_pending_inputs(limit: int = 10):
    res = (
        supabase
        .table("news_revalidation_input")
        .select("symbol, base_score, news_block")
        .eq("processed", False)
        .limit(limit)
        .execute()
    )
    return res.data or []

# ==================================================
# RUN AI
# ==================================================

ALLOWED_BIAS = {
    "strong_bullish",
    "bullish",
    "neutral",
    "bearish",
    "strong_bearish",
    "high_risk_unclear"
}

def run_ai(symbol: str, base_score: int, news_block: str):
    prompt = PROMPT_TEMPLATE.format(
        symbol=symbol,
        base_score=base_score,
        news_block=news_block
    )

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        temperature=0.2,
        messages=[
            {"role": "system", "content": "You are a financial analyst AI. Return ONLY valid JSON."},
            {"role": "user", "content": prompt}
        ]
    )

    raw = response.choices[0].message.content or ""
    raw = raw.replace("```json", "").replace("```", "").strip()

    log(f"RAW AI RESPONSE ({symbol}): {raw}")

    try:
        data = json.loads(raw)

        # 🔧 FIX: normalize keys (remove whitespace / newlines)
        data = {k.strip(): v for k, v in data.items()}

    except Exception as e:
        log(f"❌ JSON parse error for {symbol}: {e}")
        return None

    # ---- Validation ----
    if data.get("symbol") != symbol:
        log(f"❌ Symbol mismatch: expected {symbol}, got {data.get('symbol')}")
        return None

    if data.get("bias_label") not in ALLOWED_BIAS:
        log(f"❌ Invalid bias_label for {symbol}: {data.get('bias_label')}")
        return None

    for field in ("bias_strength", "updated_total_score"):
        if not isinstance(data.get(field), int):
            log(f"❌ Invalid {field} for {symbol}: {data.get(field)}")
            return None

    # ---- Explanation validation ----
    if not isinstance(data.get("explanation_text"), str):
        log(f"❌ Missing explanation_text for {symbol}")
        return None

    explanation = data.get("explanation_text", "").strip()

    word_count = len(re.findall(r"\b\w+\b", explanation))

    word_count = len(re.findall(r"\b\w+\b", explanation))

    if word_count < 120:
        log(f"❌ explanation_text too short for {symbol} ({word_count} words)")
        return None

    if word_count > 160:  # אופציונלי, אם אתה רוצה תקרה
        log(f"❌ explanation_text too long for {symbol} ({word_count} words)")
        return None

    return data

def insert_revalidation_result(
    symbol: str,
    base_score: int,
    bias_label: str,
    bias_strength: int,
    updated_total_score: int,
    explanation_text: str,
    ai_version: str
):
    supabase.table("news_analyst_revalidation_results").insert({
        "symbol": symbol,
        "base_score": base_score,
        "bias_label": bias_label,
        "bias_strength": bias_strength,
        "updated_total_score": updated_total_score,
        "explanation_text": explanation_text,
        "ai_version": ai_version,
        "created_at": datetime.now(timezone.utc).isoformat()
    }).execute()


# ==================================================
# MAIN
# ==================================================

def main():
    log("Starting AI Revalidation Step 2.6")

    while True:
        rows = fetch_pending_inputs(limit=10)

        if not rows:
            log("No more pending inputs — exiting loop")
            break

        log(f"Fetched {len(rows)} pending inputs")

        for row in rows:
            symbol = row["symbol"]
            base_score = row["base_score"]
            news_block = row["news_block"]

            log(f"Running AI for {symbol} | base_score={base_score}")

            result = run_ai(symbol, base_score, news_block)
            if not result:
                log(f"❌ AI failed for {symbol} — marking as processed to avoid loop")

                supabase.table("news_revalidation_input") \
                    .update({
                    "processed": True,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "error": "ai_validation_failed"
                }) \
                    .eq("symbol", symbol) \
                    .execute()

                continue

            log(f"✅ AI RESULT FINAL ({symbol}): {json.dumps(result, ensure_ascii=False)}")

            insert_revalidation_result(
                symbol=symbol,
                base_score=base_score,
                bias_label=result["bias_label"],
                bias_strength=result["bias_strength"],
                updated_total_score=result["updated_total_score"],
                explanation_text=result["explanation_text"],
                ai_version=APP_VERSION
            )

            # ✅ mark as processed
            supabase.table("news_revalidation_input") \
                .update({
                "processed": True,
                "processed_at": datetime.now(timezone.utc).isoformat()
            }) \
                .eq("symbol", symbol) \
                .execute()


        log("Batch completed — checking for more inputs")

    log("Finished AI Revalidation Step 2.6")


if __name__ == "__main__":
    main()
