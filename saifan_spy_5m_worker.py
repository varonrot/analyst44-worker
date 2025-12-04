import requests
from supabase import create_client, Client

# -----------------------------
# CONFIG
# -----------------------------
FMP_API_KEY = "ZT73H6uSdo3b3kAmcjPxH5EGX0odj7MJ"
SUPABASE_URL = "https://analkikdsytxkavvmulf.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFuYWxraWtkc3l0eGthdnZtdWxmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NDgyNTI2MCwiZXhwIjoyMDgwNDAxMjYwfQ.mochVG2SkWq8ytaMkYTwZsSyyTEkbbvpupIesDQ5j3E"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE = "saifan_intraday_candles_spy_5m"

# -----------------------------
# Fetch latest SPY 5-minute bar
# -----------------------------
def fetch_spy_5m():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    r = requests.get(url)
    data = r.json()

    if not isinstance(data, list) or len(data) == 0:
        print("No data from FMP.")
        return None

    return data[0]  # latest bar

# -----------------------------
# Insert raw candle to Supabase
# -----------------------------
def insert_candle(bar):
    candle = {
        "candle_time": bar["date"],
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"]
    }

    resp = supabase.table(TABLE).insert(candle).execute()
    print("Inserted:", resp)

# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main():
    bar = fetch_spy_5m()
    if not bar:
        return

    insert_candle(bar)

if __name__ == "__main__":
    main()
