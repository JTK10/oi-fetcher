import requests
import boto3
import os
import json
from datetime import datetime
import sys

DDB_TABLE = os.getenv("DYNAMODB_TABLE", "NSE_OI_DATA")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/"
}

NSE_HOLIDAYS_2026 = {
    "2026-01-15",
    "2026-01-26",
    "2026-03-06",
    "2026-03-30",
    "2026-04-14",
    "2026-05-01",
    "2026-08-15",
    "2026-10-02",
    "2026-11-12",
}

def is_nse_holiday():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return today in NSE_HOLIDAYS_2026, today

def fetch_nse_oi():
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get("https://www.nseindia.com", timeout=10)

    url = "https://www.nseindia.com/api/liveEquity-derivatives?index=stock_fut"
    r = s.get(url, timeout=10)

    if r.status_code != 200:
        print("NSE blocked the request. Skipping.")
        sys.exit(0)

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
