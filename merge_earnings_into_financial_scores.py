import os
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

NEWS_WEIGHT = 0.65
BASE_WEIGHT = 0.35

# ✅ Map DB labels -> Frontend labels
BIAS_LABEL_TO_UI = {
    "strong_bullish": "Strong Bullish Bias",
    "bullish": "Bullish Bias",
    "neutral": "Neutral / Mixed",
    "bearish": "Bearish Bias",
    "strong_bearish": "Strong Bearish Bias",
    "high_risk_unclear": "High Risk / Unclear",
}

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
    ) or []

    for r in rows:
        # weights
        final_weighted_score = round(
            (r["base_score"] * BASE_WEIGHT) +
            (r["updated_total_score"] * NEWS_WEIGHT)
        )

        raw_label = (r.get("bias_label") or "").strip()
        ui_label = BIAS_LABEL_TO_UI.get(raw_label, "Neutral / Mixed")  # fallback safe

        supabase.table("analyst_financial_scores") \
            .update({
                # ✅ store UI-ready label
                "news_bias_label": ui_label,

                # keep numeric strength 그대로 (1–100)
                "news_bias_strength": int(r["bias_strength"]),

                # news score is what AI returned
                "news_score": int(r["updated_total_score"]),

                # final weighted score
                "final_weighted_score": int(final_weighted_score),

               "explanation_text": r.get("explanation_text"),
                # timestamp
                "news_updated_at": datetime.now(timezone.utc).isoformat()
            }) \
            .eq("symbol", r["symbol"]) \
            .execute()

    print("✅ Earnings merge completed (label mapped to UI)")

if __name__ == "__main__":
    run_earnings_merge()
