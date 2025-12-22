import subprocess
import sys
import traceback
from datetime import datetime


def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)


def run_step(name: str, cmd: list[str]) -> bool:
    log(f"Starting step: {name} | command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        log(f"✔️ Step completed: {name}")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Step failed: {name} | returncode={e.returncode}")
        log(traceback.format_exc())
        return False
    except Exception:
        log(f"❌ Unexpected error in step: {name}")
        log(traceback.format_exc())
        return False


def main() -> int:
    log("🚀 analyst44worker.py started")

    # Step 1: Fetch financial statements
    if not run_step(
        "financial_statements",
        ["python3", "analyst_financial_statements_worker.py"],
    ):
        log("Stopping pipeline because financial statements step failed.")
        return 1

    # Step 1.09: Reset SPY daily bars table
    if not run_step(
            "spy_daily_bars_reset",
            ["python3", "spy_daily_bars_reset.py"],
    ):
        log("Stopping pipeline because spy_daily_bars_reset failed.")
        return 1

    # 🔹 Step 1.1: Sync SPY daily bars (6 months, FMP)
    if not run_step(
        "spy_daily_bars_sync",
        ["python3", "spy_daily_bars_sync.py"],
    ):
        log("Stopping pipeline because spy_daily_bars_sync failed.")
        return 1

    # 🔹 Step 1.15: SPY market state AI decision
    if not run_step(
            "spy_market_state_daily",
            ["python3", "spy_market_state_daily_runner.py"],
    ):
        log("Stopping pipeline because spy_market_state_daily failed.")
        return 1

    # 🔄 Step 1.16: Reset VIX daily bars table
    if not run_step(
            "vix_daily_bars_reset",
            ["python3", "vix_daily_reset.py"],
    ):
        log("Stopping pipeline because vix_daily_bars_reset failed.")
        return 1

    # 📈 Step 1.17: Sync VIX daily bars (6 months, FMP)
    if not run_step(
            "vix_daily_bars_sync",
            ["python3", "vix_daily_history_loader.py"],
    ):
        log("Stopping pipeline because vix_daily_bars_sync failed.")
        return 1

    # Step 1.18: VIX market state AI decision
    run_step(
        "vix_market_state_daily",
        ["python3", "vix_market_state_daily_runner.py"]
    )

    # 🔹 Step 1.2: Fetch earnings-related news
    if not run_step(
            "fetch_earnings_news",
            ["python3", "fmp_earnings_news_fetcher.py"],
    ):
        log("Stopping pipeline because fetch_earnings_news failed.")
        return 1

    # 🔹 Step 1.5: Reset daily scores snapshot
    if not run_step(
        "reset_financial_scores",
        ["python3", "reset_analyst_financial_scores.py"],
    ):
        log("Stopping pipeline because reset_financial_scores failed.")
        return 1

    # Step 2: Run financial scoring (build fresh snapshot)
    if not run_step(
        "financial_scores",
        ["python3", "analyst_financial_scores_worker.py"],
    ):
        log("Score step finished (may include errors).")

    # Step 2.5: Build News Revalidation Input
    if not run_step(
            "news_revalidation_input_builder",
            ["python3", "news_revalidation_input_builder.py"],
    ):
        log("Stopping pipeline because news_revalidation_input_builder failed.")
        return 1

    # Step 3: Save score history
    run_step(
        "build_scores_history",
        ["python3", "build_scores_history.py"],
    )

    # Step 3.5: Cleanup earnings calendar
    run_step(
        "cleanup_earnings_calendar",
        ["python3", "cleanup_earnings_calendar.py"],
    )

    # Step 4: Sync earnings calendar
    if not run_step(
        "update_earnings_calendar",
        ["python3", "earnings_calendar_us_sync_reset.py"],
    ):
        log("Stopping pipeline because earnings calendar update failed.")
        return 1

    # Step 5: Backfill missing earnings symbols
    run_step(
        "backfill_missing_earnings",
        ["python3", "earnings_calendar_us_backfill.py"],
    )

    log("🎯 analyst44 daily pipeline finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
