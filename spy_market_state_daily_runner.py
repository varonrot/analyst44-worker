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

# Remove markdown code fences if present
if cleaned.startswith("```"):
    cleaned = cleaned.strip("`").strip()
    if cleaned.lower().startswith("json"):
        cleaned = cleaned[4:].strip()

try:
    result = json.loads(cleaned)
except json.JSONDecodeError as e:
    raise Exception(
        f"Failed to parse JSON from LLM response:\n{raw_content}"
    ) from e

# =============================
# BASIC STRUCTURE VALIDATION
# =============================
required_keys = {
    "symbol",
    "market_state",
    "decision_strength",
    "short_explanation_8_words",
}

missing = required_keys - result.keys()
if missing:
    raise Exception(f"LLM response missing keys: {missing}")

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
