import os
import json
from datetime import date
from supabase import create_client, Client
from openai import OpenAI

# =============================
# CONFIG
# =============================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

assert SUPABASE_URL and SUPABASE_KEY and OPENAI_API_KEY

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

PROMPT_FILE = "vix_market_state_prompt.txt"
SYMBOL = "VIX"

# =============================
# HELPERS
# =============================

def load_prompt() -> str:
    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        return f.read()

def fetch_vix_daily(limit=180):
    res = (
        supabase.table("vix_daily")
        .select("trade_date, open, high, low, close")
        .order("trade_date", desc=False)
        .limit(limit)
        .execute()
    )
    return res.data or []

def fetch_previous_decision():
    res = (
        supabase.table("vix_market_state_history")
        .select("market_state, decision_strength, decision_date, explanation")
        .order("decision_date", desc=True)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None

def call_llm(prompt: str, payload: dict) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.1,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(payload)}
        ]
    )

    content = response.choices[0].message.content.strip()
    return json.loads(content)

def insert_decision(result: dict):
    supabase.table("vix_market_state_history").insert({
        "symbol": SYMBOL,
        "decision_date": date.today().isoformat(),
        "market_state": result["market_state"],
        "decision_strength": int(result["decision_strength"]),
        "explanation": result["explanation"],
        "source": "chatgpt-vix"
    }).execute()

# =============================
# MAIN
# =============================

def main():
    print("▶️ VIX market state runner started")

    vix_data = fetch_vix_daily()
    if len(vix_data) < 30:
        raise Exception("Not enough VIX data")

    previous_decision = fetch_previous_decision()

    payload = {
        "vix_daily_data": vix_data,
        "previous_vix_decision": previous_decision
    }

    prompt = load_prompt()
    result = call_llm(prompt, payload)

    insert_decision(result)

    print("✅ VIX market state stored:", result["market_state"])

if __name__ == "__main__":
    main()
