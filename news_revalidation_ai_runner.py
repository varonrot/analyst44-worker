import os
import json
import re
from datetime import datetime
from supabase import create_client, Client
from openai import OpenAI

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

    return data

# ==================================================
# MAIN
# ==================================================

def main():
    log("Starting AI Revalidation Step 2.6")

    rows = fetch_pending_inputs(limit=10)
    log(f"Fetched {len(rows)} pending inputs")

    for row in rows:
        symbol = row["symbol"]
        base_score = row["base_score"]
        news_block = row["news_block"]

        log(f"Running AI for {symbol} | base_score={base_score}")

        result = run_ai(symbol, base_score, news_block)
        if not result:
            log(f"❌ AI failed for {symbol}")
            continue

        # 🔥 ONLY LOG — NO DB WRITE
        log(f"✅ AI RESULT FINAL ({symbol}): {json.dumps(result, ensure_ascii=False)}")

    log("Finished AI Revalidation Step 2.6")

if __name__ == "__main__":
    main()
