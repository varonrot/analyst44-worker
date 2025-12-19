import os
import requests
from datetime import datetime
from supabase import create_client, Client

FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

FMP_URL = "https://financialmodelingprep.com/api/v3/stock_news"

def fetch_news():
    params = {
        "limit": 50,
        "apikey": FMP_API_KEY
    }

    r = requests.get(FMP_URL, params=params, timeout=20)
    r.raise_for_status()
    news = r.json()

    print(f"Fetched {len(news)} news items")

    inserted = 0

    for item in news:
        row = {
            "symbol": item.get("symbol"),
            "title": item.get("title"),
            "body": item.get("text"),
            "site": item.get("site"),
            "url": item.get("url"),
            "published_at": item.get("publishedDate"),
            "fetched_at": datetime.utcnow().isoformat()
        }

        try:
            supabase.table("fmp_news") \
                .upsert(row, on_conflict="url") \
                .execute()
            inserted += 1
        except Exception as e:
            print("Failed to upsert:", e)

    print(f"Upserted {inserted} rows")

if __name__ == "__main__":
    fetch_news()
