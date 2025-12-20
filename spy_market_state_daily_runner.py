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
    raise Exception("Not enough daily bars to evaluate market state")

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

Your task is to determine the CURRENT market state.

You MUST choose EXACTLY ONE state from the list below:

1. Strong Uptrend
2. Weak Uptrend
3. Range / Balanced
4. Weak Downtrend
5. Strong Downtrend
6. Transition / Distribution
7. High Volatility / Unstable

Critical rules:
- Choose exactly ONE market state
- Do NOT invent new states
- Do NOT change the market state from recent history unless current evidence is CLEAR and STRONG
- Favor continuity and stability over frequent changes
- Base decisions strictly on price structure, volatility behavior, and swing consistency
- Do NOT reference technical indicators by name

Return the result in the following JSON format ONLY:

{{
  "symbol": "SPY",
  "market_state": "<ONE_STATE_FROM_LIST>",
  "decision_strength": <0-100>,
  "short_explanation_8_words": "<MAX 8 WORDS>"
}}

Data:
{json.dumps(bars, indent=2)}

Previous Decisions:
{json.dumps(previous_decisions, indent=2)}
"""

# =============================
# CALL OPENAI
# =============================
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    temperature=0
)

content = response.choices[0].message.content.strip()

result = json.loads(content)

# =============================
# UPSERT DECISION
# =============================
today = date.today().isoformat()

supabase.table("spy_market_state_history").upsert(
    {
        "symbol": SYMBOL,
        "decision_date": today,
        "market_state": result["market_state"],
        "decision_strength": int(result["decision_strength"]),
        "explanation": result["short_explanation_8_words"],
    },
    on_conflict="symbol,decision_date"
).execute()

print("✔️ SPY market state decision saved:", result)
