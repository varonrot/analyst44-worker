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
            created_at
        """)
        .execute()
        .data
    )

    for r in rows:
        final_weighted_score = round(
            r["base_score"] * BASE_WEIGHT +
            r["updated_total_score"] * NEWS_WEIGHT
        )

        supabase.table("analyst_financial_scores") \
            .update({
                "news_bias_label": r["bias_label"],
                "news_bias_strength": r["bias_strength"],
                "news_score": r["updated_total_score"],
                "final_weighted_score": final_weighted_score,
                "news_updated_at": datetime.now(timezone.utc).isoformat()
            }) \
            .eq("symbol", r["symbol"]) \
            .execute()

    print("✅ Earnings merge completed (news → final_weighted_score)")


if __name__ == "__main__":
    run_earnings_merge()
