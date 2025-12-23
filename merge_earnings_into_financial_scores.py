import os
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

NEWS_WEIGHT = 0.65
BASE_WEIGHT = 0.35


def run_earnings_merge():
    rows = (
        supabase
        .table("news_analyst_revalidation_results")
        .select("""
            symbol,
            base_score,
            updated_total_score,
            bias_label,
            bias_strength,
            explanation_text,
            created_at
        """)
        .execute()
        .data
    )

    for r in rows:
        final_score = round(
            r["base_score"] * BASE_WEIGHT +
            r["updated_total_score"] * NEWS_WEIGHT
        )

        supabase.table("analyst_financial_scores") \
            .update({
                "total_score": final_score,
                "earnings_bias_label": r["bias_label"],
                "earnings_bias_strength": r["bias_strength"],
                "earnings_explanation_text": r["explanation_text"],
                "analysis_date": datetime.now(timezone.utc).date().isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }) \
            .eq("symbol", r["symbol"]) \
            .execute()

    print("✅ Earnings merge completed")


if __name__ == "__main__":
    run_earnings_merge()
