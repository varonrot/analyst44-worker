import os
from datetime import datetime
from supabase import create_client, Client

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

def log(msg: str):
    ts = datetime.utcnow().isoformat()
    print(f"[NEWS-INPUT] {ts} | {msg}")

# ==================================================
# STEP 1 – SYMBOLS FROM NEWS
# ==================================================

def get_symbols_with_news():
    res = supabase.table("fmp_news").select("symbol").execute()
    if not res.data:
        return []

    return sorted({r["symbol"] for r in res.data if r.get("symbol")})

# ==================================================
# STEP 2 – COLLECT NEWS
# ==================================================

def collect_news_block(symbol: str, limit: int = 5) -> str:
    res = (
        supabase
        .table("fmp_news")
        .select("site, title, body")
        .eq("symbol", symbol)
        .order("published_at", desc=True)
        .limit(limit)
        .execute()
    )

    if not res.data:
        return ""

    block = []
    for i, row in enumerate(res.data, start=1):
        if not row.get("title") or not row.get("body"):
            continue

        block.append(
            f"""[{i}]
Source: {row.get("site")}
Title: {row.get("title")}
Body: {row.get("body")}
"""
        )

    return "\n".join(block).strip()

# ==================================================
# STEP 3 – BASELINE
# ==================================================

def get_latest_baseline(symbol: str):
    res = (
        supabase
        .table("analyst_financial_scores")
        .select("analysis_date, total_score")
        .eq("symbol", symbol)
        .order("analysis_date", desc=True)
        .limit(1)
        .execute()
    )

    if not res.data:
        return None

    row = res.data[0]
    if row.get("total_score") is None:
        return None

    return row

# ==================================================
# STEP 4 – UPSERT INPUT TABLE
# ==================================================

def upsert_news_revalidation_input(
    symbol: str,
    analysis_date,
    base_score: int,
    news_block: str
):
    payload = {
        "symbol": symbol,
        "analysis_date": analysis_date,
        "base_score": base_score,
        "news_block": news_block,
        "processed": False,
        "created_at": datetime.utcnow().isoformat()
    }

    supabase.table("news_revalidation_input") \
        .upsert(payload, on_conflict="symbol,analysis_date") \
        .execute()

# ==================================================
# MAIN
# ==================================================

def main():
    log("Starting news revalidation input builder")

    symbols = get_symbols_with_news()
    log(f"Found {len(symbols)} symbols with news")

    for symbol in symbols:
        baseline = get_latest_baseline(symbol)
        if not baseline:
            log(f"Skipping {symbol} – no baseline")
            continue

        news_block = collect_news_block(symbol)
        if not news_block:
            log(f"Skipping {symbol} – empty news block")
            continue

        upsert_news_revalidation_input(
            symbol=symbol,
            analysis_date=baseline["analysis_date"],
            base_score=baseline["total_score"],
            news_block=news_block
        )

        log(f"Prepared AI input for {symbol}")

    log("Finished building news revalidation input")

if __name__ == "__main__":
    main()
