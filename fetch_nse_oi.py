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
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

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
    weekday = datetime.now().weekday()
    is_weekend = weekday >= 5
    return (today_str in NSE_HOLIDAYS_2026) or is_weekend, today_str

def create_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        print("   -> Initializing session (getting cookies)...")
        s.get("https://www.nseindia.com", timeout=15)
        return s
    except Exception as e:
        print(f"   -> Error initializing session: {e}")
        sys.exit(1)

def fetch_data(session, url, referer):
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
    # STEP 1: Fetch Master Price List (For better price data)
    # ---------------------------------------------------------
    print("1. Fetching Master Stock Futures List...")
    url_master = "https://www.nseindia.com/api/liveEquity-derivatives?index=stock_fut"
    master_resp = fetch_data(s, url_master, "https://www.nseindia.com/market-data/equity-derivatives-watch")
    
    master_map = {}
    if master_resp and "data" in master_resp:
        for item in master_resp["data"]:
            sym = item.get("underlying")
            if sym:
                master_map[sym] = item
        print(f"   -> Got {len(master_map)} stocks from Master List.")

    # ---------------------------------------------------------
    # STEP 2: Fetch OI Spurts
    # ---------------------------------------------------------
    print("2. Fetching OI Spurts...")
    url_oi = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    oi_resp = fetch_data(s, url_oi, "https://www.nseindia.com/market-data/oi-spurts")
    
    oi_list = []
    if oi_resp and "data" in oi_resp:
        oi_list = oi_resp["data"]
        print(f"   -> Got {len(oi_list)} OI Spurt records.")
    else:
        print("   -> WARNING: Could not fetch OI Spurts.")

    # ---------------------------------------------------------
    # STEP 3: UNION MERGE & CALCULATION (The Fix)
    # ---------------------------------------------------------
    print("3. Merging datasets & Calculating %...")
    final_map = {}

    for oi_item in oi_list:
        sym = oi_item.get("symbol")
        if sym:
            # --- FIX: Extract Correct Fields based on Debug Logs ---
            # Keys found: 'latestOI', 'prevOI', 'changeInOI', 'underlyingValue'
            
            latest_oi = float(oi_item.get("latestOI", 0) or 0)
            prev_oi   = float(oi_item.get("prevOI", 0) or 0)
            change_oi = float(oi_item.get("changeInOI", 0) or 0)
            last_price = float(oi_item.get("underlyingValue", 0) or 0)

            # --- FIX: Calculate % Change Manually ---
            # Because API doesn't provide it directly
            p_change_oi = 0.0
            if prev_oi > 0:
                p_change_oi = (change_oi / prev_oi) * 100
            
            final_map[sym] = {
                "underlying": sym,
                "symbol": sym,
                # Standardized Keys for Lambda
                "pChangeInOpenInterest": round(p_change_oi, 2),
                "changeinOpenInterest": change_oi,
                "openInterest": latest_oi,
                "lastPrice": last_price,
                "source": "OI_SPURTS"
            }

    # Overlay Master Data (to fill in gaps if any)
    for sym, master_item in master_map.items():
        if sym in final_map:
            # Preserve our manually calculated OI values
            # (Master data often has outdated or 0 OI)
            saved_pct = final_map[sym]["pChangeInOpenInterest"]
            saved_chg = final_map[sym]["changeinOpenInterest"]
            saved_oi  = final_map[sym]["openInterest"]
            
            final_map[sym].update(master_item)
            
            # Force restore the good OI data
            final_map[sym]["pChangeInOpenInterest"] = saved_pct
            final_map[sym]["changeinOpenInterest"] = saved_chg
            final_map[sym]["openInterest"] = saved_oi
            final_map[sym]["source"] = "MERGED"
        else:
            # If purely from Master, try to find fields (unlikely to have OI change)
            master_item["pChangeInOpenInterest"] = 0
            master_item["changeinOpenInterest"] = 0
            final_map[sym] = master_item
            final_map[sym]["source"] = "MASTER_ONLY"

    final_data = list(final_map.values())
    print(f"   -> Final Merged Count: {len(final_data)} records.")
    
    return {"data": final_data, "timestamp": datetime.now().isoformat()}

def save_to_dynamodb(json_data):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DDB_TABLE)

        item = {
            "PK": "NSE#OI",
            "SK": "LATEST",
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
