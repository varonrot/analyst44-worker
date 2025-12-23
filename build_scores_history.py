import os
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def build_history():
    print("Fetching analyst_financial_scores...")

    response = supabase.table("analyst_financial_scores").select("*").execute()

    if not response.data:
        print("No rows found.")
        return

    rows = response.data
    print(f"Inserting {len(rows)} rows into history...")

    for row in rows:
        history_row = {
            "original_id": row["id"],
            "symbol": row["symbol"],
            "last_earnings_date": row["last_earnings_date"],
            "analysis_date": row["analysis_date"],
            "echo_price": row["echo_price"],

            # --- core scores ---
            "total_score": row["total_score"],
            "profitability": row["profitability"],
            "growth": row["growth"],
            "financial_strength": row["financial_strength"],

            # --- targets ---
            "target_range_low": row["target_range_low"],
            "target_range_high": row["target_range_high"],

            # --- AI text ---
            "swing_forecast_weeks_2_3": row["swing_forecast_weeks_2_3"],
            "volatility_flag": row["volatility_flag"],
            "summary_30_words": row["summary_30_words"],
            "comparison_trend": row["comparison_trend"],
            "direction": row["direction"],

            # --- 🔥 NEWS / EARNINGS FIELDS (NEW) ---
            "news_bias_label": row["news_bias_label"],
            "news_bias_strength": row["news_bias_strength"],
            "news_score": row["news_score"],
            "final_weighted_score": row["final_weighted_score"],
            "news_updated_at": row["news_updated_at"],

            # --- meta ---
            "saved_at": datetime.utcnow().isoformat()
        }

        supabase.table("analyst_financial_scores_history").insert(history_row).execute()
        print(f"Inserted: {row['symbol']}")

    print("DONE.")

if __name__ == "__main__":
    build_history()
