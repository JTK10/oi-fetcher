import requests
import boto3
import os
import json
from datetime import datetime
import pytz
import sys

# ===== ENV VARS =====
DDB_TABLE = os.getenv("DYNAMODB_TABLE", "NSE_OI_DATA")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/"
}

# ===== NSE HOLIDAYS (IST) =====
NSE_HOLIDAYS_2026 = {
    "2026-01-15",  # YOUR ADDED HOLIDAY
    "2026-01-26",  # Republic Day
    "2026-03-06",  # Holi
    "2026-03-30",  # Ram Navami
    "2026-04-14",  # Dr Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-08-15",  # Independence Day
    "2026-10-02",  # Gandhi Jayanti
    "2026-11-12",  # Diwali (example)
}

# =========================
# FUNCTIONS
# =========================

def is_nse_holiday():
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).strftime("%Y-%m-%d")
    return today in NSE_HOLIDAYS_2026, today


def fetch_nse_oi():
    print("Connecting to NSE...")

    s = requests.Session()
    s.headers.update(HEADERS)

    # Step 1: Set cookies
    s.get("https://www.nseindia.com", timeout=10)

    # Step 2: Fetch Stock Futures OI
    url = "https://www.nseindia.com/api/liveEquity-derivatives?index=stock_fut"
    r = s.get(url, timeout=10)
    r.raise_for_status()

    return r.json()


def save_to_dynamodb(data):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_TABLE)

    item = {
        "PK": "NSE#OI",
        "SK": datetime.utcnow().isoformat(),
        "data": json.dumps(data)
    }

    table.put_item(Item=item)


# =========================
# MAIN
# =========================

if __name__ == "__main__":

    is_holiday, today = is_nse_holiday()

    if is_holiday:
        print(f"Today ({today}) is an NSE Holiday. Skipping run.")
        sys.exit(0)

    print("Fetching NSE OI...")
    data = fetch_nse_oi()

    print("Saving to DynamoDB...")
    save_to_dynamodb(data)

    print("Done successfully.")
