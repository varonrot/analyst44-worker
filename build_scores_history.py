import os
from supabase import create_client, Client
from datetime import datetime

# --- Supabase init ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def build_history():
    print("Fetching current scores...")

    # Fetch all rows from the main scores table
    response = supabase.table("analyst_financial_scores").select("*").execute()

    if response.data is None or len(response.data) == 0:
        print("No rows found. Exiting.")
        return

    rows = response.data
    history_rows = []

    print(f"Found {len(rows)} rows. Preparing history objects...")

    for row in rows:
        history_rows.append({
            "original_id": row["id"],
            "symbol": row["symbol"],
            "last_earnings_date": row["last_earnings_date"],
            "analysis_date": row["analysis_date"],
            "echo_price": row["echo_price"],
            "total_score": row["total_score"],
            "profitability": row["profitability"],
            "growth": row["growth"],
            "financial_strength": row["financial_strength"],
            "target_range_low": row["target_range_low"],
            "target_range_high": row["target_range_high"],
            "swing_forecast_weeks_2_3": row["swing_forecast_weeks_2_3"],
            "volatility_flag": row["volatility_flag"],
            "summary_30_words": row["summary_30_words"],
            "comparison_trend": row["comparison_trend"],
            "created_at": row["created_at"],
            "direction": row["direction"],
            "saved_at": datetime.utcnow().isoformat()
        })

    print("Inserting into history table...")

    # Insert in batches of 1000 if needed
    batch_size = 500
    for i in range(0, len(history_rows), batch_size):
        batch = history_rows[i:i+batch_size]
        supabase.table("analyst_financial_scores_history").insert(batch).execute()

    print("History build complete.")

if __name__ == "__main__":
    build_history()
