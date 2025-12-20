import os
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client

# =============================
# CONFIG
# =============================
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([FMP_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    raise Exception("Missing environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SYMBOL = "SPY"
DAYS_BACK = 190  # ~6 months including buffer

# =============================
# FETCH DATA FROM FMP
# =============================
url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{SYMBOL}?apikey={FMP_API_KEY}"
response = requests.get(url)
data = response.json()

if "historical" not in data:
    raise Exception("No historical data returned from FMP")

cutoff_date = datetime.utcnow() - timedelta(days=DAYS_BACK)

rows = []
for bar in data["historical"]:
    bar_date = datetime.strptime(bar["date"], "%Y-%m-%d")

    if bar_date < cutoff_date:
        continue

    rows.append({
        "symbol": SYMBOL,
        "bar_date": bar["date"],
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"]
    })

if not rows:
    print("No rows to insert")
    exit()

# =============================
# UPSERT INTO SUPABASE
# =============================
result = supabase.table("spy_daily_bars").upsert(
    rows,
    on_conflict="symbol,bar_date"
).execute()

print(f"Upserted {len(rows)} daily bars into spy_daily_bars")
