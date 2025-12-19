import os
import requests
from datetime import datetime
from supabase import create_client, Client

FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

FMP_URL = "https://financialmodelingprep.com/api/v3/stock_news"

def get_earnings_symbols():
    res = (
        supabase
        .table("earnings_calendar_us")
        .select("symbol, earnings_date")
        .execute()
    )
    return res.data or []

def fetch_news_for_symbol(symbol, earnings_date):
    params = {
        "symbol": symbol,
        "limit": 20,
        "apikey": FMP_API_KEY
    }

    r = requests.get(FMP_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def main():
    earnings = get_earnings_symbols()
    print(f"Found {len(earnings)} earnings symbols")

    for item in earnings:
        symbol = item["symbol"]
        earnings_date = item["earnings_date"]

        try:
            news_list = fetch_news_for_symbol(symbol, earnings_date)
        except Exception as e:
            print(f"Failed fetching news for {symbol}: {e}")
            continue

        for news in news_list:
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

        print(f"{symbol}: processed {len(news_list)} news items")

if __name__ == "__main__":
    main()
