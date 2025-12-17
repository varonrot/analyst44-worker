import os
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def build_history():
    print("Fetching current scores...")
    response = supabase.table("analyst_financial_scores").select("*").execute()

    if not response.data:
        print("No rows found. Exiting.")
        return

    print(f"Found {len(response.data)} rows")

    for row in response.data:
        try:
            history_row = {
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
                "direction": row["direction"],
                "saved_at": datetime.utcnow().isoformat()
            }

            supabase.table("analyst_financial_scores_history") \
                .insert(history_row) \
                .execute()

            print(f"Inserted {row['symbol']} | {row['last_earnings_date']}")

        except Exception as e:
            print(f"FAILED {row['symbol']}: {e}")

    print("History build complete.")

if __name__ == "__main__":
    build_history()
