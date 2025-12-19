import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def reset_scores():
    print("Resetting analyst_financial_scores...")

    # חובה WHERE – תופס את כל השורות
    res = (
        supabase
        .table("analyst_financial_scores")
        .delete()
        .neq("id", 0)   # <-- הטריק: WHERE שתמיד true
        .execute()
    )

    deleted = len(res.data) if res.data else 0
    print(f"Deleted rows: {deleted}")

if __name__ == "__main__":
    reset_scores()
