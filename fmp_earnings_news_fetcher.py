import os
import requests
from datetime import datetime
from supabase import create_client, Client

# =============================
# CONFIG
# =============================

FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

FMP_URL = "https://financialmodelingprep.com/api/v3/stock_news"

# =============================
# EARNINGS-RELATED FILTER
# =============================

EARNINGS_KEYWORDS = [
    "earnings",
    "quarter",
    "q1", "q2", "q3", "q4",
    "revenue",
    "eps",
    "guidance",
    "outlook",
    "results",
    "fiscal",
    "conference call",
    "transcript",
    "beats",
    "misses"
]

NEGATIVE_KEYWORDS = [
    "dividend",
    "shares purchased",
    "shares sold",
    "position cut",
    "position increased",
    "insider",
    "hedge fund",
    "etf",
    "top stocks",
    "best stocks",
    "million investment",
    "acquired",
    "sold by",
    "retirement system",
    "wealth advisors"
]

def is_earnings_related(news: dict, earnings_date: str | None, symbol: str) -> bool:
    title = news.get("title", "").lower()
    text = f"{title} {news.get('text','')}".lower()

    # 1. החברה חייבת להיות בכותרת
    if symbol.lower() not in title:
        return False

    # 2. חייב מילות Earnings
    if not any(k in text for k in EARNINGS_KEYWORDS):
        return False

    # 3. מסנן רעש ידוע
    if any(k in text for k in NEGATIVE_KEYWORDS):
        return False

    # 4. חלון זמן ±3 ימים
    if earnings_date and news.get("publishedDate"):
        try:
            pub = datetime.fromisoformat(news["publishedDate"][:19])
            earn = datetime.fromisoformat(earnings_date)
            if abs((pub - earn).days) > 3:
                return False
        except Exception:
            pass

    return True


# =============================
# DATA SOURCES
# =============================

def get_earnings_symbols():
    res = (
        supabase
        .table("earnings_calendar_us")
        .select("symbol, report_date")
        .execute()
    )
    return res.data or []

def fetch_news_for_symbol(symbol: str):
    params = {
        "symbol": symbol,
        "limit": 20,
        "apikey": FMP_API_KEY
    }

    r = requests.get(FMP_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# =============================
# MAIN
# =============================

def main():
    earnings = get_earnings_symbols()
    print(f"Found {len(earnings)} earnings symbols")

    for item in earnings:
        symbol = item["symbol"]
        earnings_date = item["report_date"]

        try:
            news_list = fetch_news_for_symbol(symbol)
        except Exception as e:
            print(f"Failed fetching news for {symbol}: {e}")
            continue

        inserted = 0

        for news in news_list:
            if not is_earnings_related(news, earnings_date, symbol):
                continue

            row = {
                "symbol": symbol,
                "title": news.get("title"),
                "body": news.get("text"),
                "site": news.get("site"),
                "url": news.get("url"),
                "published_at": news.get("publishedDate"),
                "earnings_date": earnings_date,
                "fetched_at": datetime.utcnow().isoformat()
            }

            supabase.table("fmp_news") \
                .upsert(row, on_conflict="url") \
                .execute()

            inserted += 1

        print(f"{symbol}: inserted {inserted} earnings-related news items")

if __name__ == "__main__":
    main()
