import supabase
import os

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

sb = supabase.create_client(url, key)

def main():
    print("Deleting all rows from earnings_calendar_us ...")
    sb.table("earnings_calendar_us").delete().neq("symbol", "__never__").execute()
    print("Done cleanup.")

if __name__ == "__main__":
    main()
