import requests
import boto3
import os
import json
import time
import sys
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
DDB_TABLE = os.getenv("DYNAMODB_TABLE", "NSE_OI_DATA")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")  # Ensure Region is set

# NSE Headers (Crucial for bypassing blocks)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/equity-derivatives-watch",
    "Connection": "keep-alive"
}

NSE_HOLIDAYS_2026 = {
    "2026-01-26", "2026-03-06", "2026-03-30", "2026-04-14",
    "2026-05-01", "2026-08-15", "2026-10-02", "2026-11-12",
}

def is_nse_holiday():
    today_str = datetime.now().strftime("%Y-%m-%d")
    # Also check for weekends (5=Saturday, 6=Sunday)
    weekday = datetime.now().weekday()
    is_weekend = weekday >= 5
    return (today_str in NSE_HOLIDAYS_2026) or is_weekend, today_str

def create_session():
    """Creates a session and initializes cookies by hitting the homepage."""
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        # 1. Hit Homepage to get cookies
        print("   -> Initializing session (getting cookies)...")
        s.get("https://www.nseindia.com", timeout=15)
        return s
    except Exception as e:
        print(f"   -> Error initializing session: {e}")
        sys.exit(1)

def fetch_data(session, url, referer):
    """Generic fetcher with referer update."""
    try:
        session.headers.update({"Referer": referer})
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"   -> Failed to fetch {url} (Status: {r.status_code})")
            return None
    except Exception as e:
        print(f"   -> Exception fetching {url}: {e}")
        return None

def get_merged_nse_data():
    s = create_session()
    
    # ---------------------------------------------------------
    # STEP 1: Fetch Master Price List (ALL F&O Stocks)
    # ---------------------------------------------------------
    print("1. Fetching Master Stock Futures List...")
    url_master = "https://www.nseindia.com/api/liveEquity-derivatives?index=stock_fut"
    master_resp = fetch_data(s, url_master, "https://www.nseindia.com/market-data/equity-derivatives-watch")
    
    if not master_resp or "data" not in master_resp:
        print("CRITICAL: Could not fetch Master List. Exiting.")
        sys.exit(1)
        
    master_list = master_resp["data"] # This contains the 180+ stocks
    print(f"   -> Got {len(master_list)} stocks from Master List.")

    # ---------------------------------------------------------
    # STEP 2: Fetch OI Spurts (Contains 'pChangeInOpenInterest')
    # ---------------------------------------------------------
    print("2. Fetching OI Spurts (for % OI Change)...")
    url_oi = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    oi_resp = fetch_data(s, url_oi, "https://www.nseindia.com/market-data/oi-spurts")
    
    # Create a lookup dictionary for OI data
    oi_lookup = {}
    if oi_resp and "data" in oi_resp:
        for item in oi_resp["data"]:
            # 'symbol' is the key in OI Spurts, 'underlying' is key in Master
            sym = item.get("symbol")
            if sym:
                oi_lookup[sym] = item
        print(f"   -> Got {len(oi_lookup)} OI Spurt records.")
    else:
        print("   -> WARNING: Could not fetch OI Spurts. Signals may be Neutral.")

    # ---------------------------------------------------------
    # STEP 3: Merge Data
    # ---------------------------------------------------------
    print("3. Merging datasets...")
    final_data = []
    
    for stock in master_list:
        symbol = stock.get("underlying")
        
        # Default values
        stock["pChangeInOpenInterest"] = 0
        stock["changeinOpenInterest"] = 0
        
        # If we have specific OI data, inject it
        if symbol in oi_lookup:
            oi_data = oi_lookup[symbol]
            # Map the fields from Spurts to the Master record
            # Note: Field names might vary slightly, we normalize them here
            stock["pChangeInOpenInterest"] = oi_data.get("pchangeinOpenInterest", 0)
            stock["changeinOpenInterest"] = oi_data.get("changeinOpenInterest", 0)
            stock["totalOpenInterest"] = oi_data.get("openInterest", stock.get("openInterest", 0))
        
        final_data.append(stock)

    print(f"   -> Merged {len(final_data)} records successfully.")
    return {"data": final_data, "timestamp": datetime.now().isoformat()}

def save_to_dynamodb(json_data):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DDB_TABLE)

        # We store the entire list as a JSON string to fit schema
        # PK = NSE#OI ensures we always overwrite/fetch the single latest record
        item = {
            "PK": "NSE#OI",
            "SK": "LATEST", # Constant SK to make querying easy (or use timestamp for history)
            "updatedAt": datetime.now().isoformat(),
            "data": json.dumps(json_data["data"]) 
        }

        table.put_item(Item=item)
        print("   -> Write to DynamoDB Successful!")
        
    except Exception as e:
        print(f"   -> DynamoDB Write Error: {e}")

if __name__ == "__main__":
    is_holiday, today_str = is_nse_holiday()

    if is_holiday:
        print(f"Today ({today_str}) is a Holiday/Weekend. Skipping.")
        sys.exit(0)

    print(f"Starting Scraper for {today_str}...")
    full_data = get_merged_nse_data()
    
    print("Saving to DynamoDB...")
    save_to_dynamodb(full_data)
    
    print("Done.")
