import supabase
import os

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

sb = supabase.create_client(url, key)

def main():
    print("Truncating earnings_calendar_us ...")
    sb.rpc("truncate_earnings_calendar").execute()
    print("Done cleanup.")

if __name__ == "__main__":
    main()
