import requests
import boto3
import os
import json
from datetime import datetime

# ===== ENV VARS =====
DDB_TABLE = os.getenv("DYNAMODB_TABLE", "NSE_OI_DATA")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/"
}

def fetch_nse_oi():
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

if __name__ == "__main__":
    print("Fetching NSE OI...")
    data = fetch_nse_oi()

    print("Saving to DynamoDB...")
    save_to_dynamodb(data)

    print("Done.")
