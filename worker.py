import time
import requests
from datetime import datetime

def main():
    while True:
        print(f"[{datetime.now()}] Analyst44 Worker is running...")
        time.sleep(60)

if __name__ == "__main__":
    main()
