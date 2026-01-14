import requests
import json
import boto3
import os

# ===== ENV VARS =====
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_KEY = "nse_oi_stock_fut.json"

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

def save_to_s3(data):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=json.dumps(data),
        ContentType="application/json"
    )

if __name__ == "__main__":
    print("Fetching NSE OI...")
    data = fetch_nse_oi()
    print("Saving to S3...")
    save_to_s3(data)
    print("Done.")
