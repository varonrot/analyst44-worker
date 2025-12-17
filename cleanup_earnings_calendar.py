import supabase
import os

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

sb = supabase.create_client(url, key)

def main():
    print("Cleaning earnings_calendar_us...")
    res = (
        sb
        .table("earnings_calendar_us")
        .delete()
        .neq("symbol", "__never__")  # תנאי דמה
        .execute()
    )
    print("Deleted rows:", len(res.data) if res.data else 0)

if __name__ == "__main__":
    main()
