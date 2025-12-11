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

    # Step 2: Run financial scoring
    if not run_step(
        "financial_scores",
        ["python3", "analyst_financial_scores_worker.py"],
    ):
        log("Score step finished (may include errors).")

    # Step 3: Save score history
    run_step(
        "build_scores_history",
        ["python3", "build_scores_history.py"],
    )

    # Step 3.5: Cleanup earnings_calendar_us table before rebuilding
    run_step(
        "cleanup_earnings_calendar",
        ["python3", "cleanup_earnings_calendar.py"],
    )

    # Step 4: Update earnings calendar table
    if not run_step(
        "update_earnings_calendar",
        ["python3", "earnings_calendar_us_sync_reset.py"],
    ):
        log("Stopping pipeline because earnings calendar update failed.")
        return 1

    log("🎯 analyst44 daily pipeline finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
