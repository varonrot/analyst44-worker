import os
import json
from datetime import date, timedelta
from supabase import create_client, Client
from openai import OpenAI

# =============================
# CONFIG
# =============================
SYMBOL = "SPY"
DAYS_BACK = 190
DECISIONS_LOOKBACK = 7

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, OPENAI_API_KEY]):
    raise Exception("Missing environment variables")

# =============================
# CLIENTS (‼️ קריטי ‼️)
# =============================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

# =============================
# FETCH DAILY BARS
# =============================
bars_resp = supabase.table("spy_daily_bars") \
    .select("bar_date, open, high, low, close, volume") \
    .gte("bar_date", date.today() - timedelta(days=DAYS_BACK)) \
    .order("bar_date", desc=False) \
    .execute()

bars = [
    {
        "date": r["bar_date"],
        "open": float(r["open"]),
        "high": float(r["high"]),
        "low": float(r["low"]),
        "close": float(r["close"]),
        "volume": int(r["volume"]),
    }
    for r in bars_resp.data
]

if len(bars) < 50:
    raise Exception("Not enough daily bars")

# =============================
# FETCH PREVIOUS DECISIONS
# =============================
decisions_resp = supabase.table("spy_market_state_history") \
    .select("decision_date, market_state, decision_strength") \
    .eq("symbol", SYMBOL) \
    .order("decision_date", desc=True) \
    .limit(DECISIONS_LOOKBACK) \
    .execute()

previous_decisions = list(reversed(decisions_resp.data or []))

# =============================
# BUILD PROMPT
# =============================
prompt = f"""
You are a professional market analyst.

You are given:
1. 6 months of DAILY OHLCV data for SPY
2. A history of recent market state decisions

Choose EXACTLY ONE market state from:
- Strong Uptrend
- Weak Uptrend
- Range / Balanced
- Weak Downtrend
- Strong Downtrend
- Transition / Distribution
- High Volatility / Unstable

Return JSON ONLY:
{{
  "symbol": "SPY",
  "market_state": "<STATE>",
  "decision_strength": <0-100>,
  "short_explanation_8_words": "<MAX 8 WORDS>"
}}

Data:
{json.dumps(bars)}

Previous Decisions:
{json.dumps(previous_decisions)}
"""

# =============================
# CALL OPENAI
# =============================
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    temperature=0
)

raw_content = response.choices[0].message.content.strip()
print("🔍 RAW LLM RESPONSE:")
print(raw_content)

# =============================
# SAFE JSON PARSING
# =============================
cleaned = raw_content
if cleaned.startswith("```"):
    cleaned = cleaned.strip("`").strip()
    if cleaned.lower().startswith("json"):
        cleaned = cleaned[4:].strip()

result = json.loads(cleaned)

# =============================
# UPSERT DECISION
# =============================
supabase.table("spy_market_state_history").upsert(
    {
        "symbol": SYMBOL,
        "decision_date": date.today().isoformat(),
        "market_state": result["market_state"],
        "decision_strength": int(result["decision_strength"]),
        "explanation": result["short_explanation_8_words"],
    },
    on_conflict="symbol,decision_date"
).execute()

print("✔️ SPY market state decision saved:", result)
