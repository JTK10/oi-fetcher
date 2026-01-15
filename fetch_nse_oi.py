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

# --- NEW: Robust Value Finder ---
def get_robust_val(item, keys_to_try):
    """Checks multiple keys to find a value, returns 0 if none found."""
    for k in keys_to_try:
        if k in item and item[k] is not None:
            try:
                # Remove commas if string formatted numbers
                val = str(item[k]).replace(',', '')
                return float(val)
            except:
                continue
    return 0

def get_merged_nse_data():
    s = create_session()
    
    # ---------------------------------------------------------
    # STEP 1: Fetch Master Price List
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
    # STEP 2: Fetch OI Spurts (Primary Source for OI)
    # ---------------------------------------------------------
    print("2. Fetching OI Spurts (for % OI Change)...")
    url_oi = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    oi_resp = fetch_data(s, url_oi, "https://www.nseindia.com/market-data/oi-spurts")
    
    oi_list = []
    if oi_resp and "data" in oi_resp:
        oi_list = oi_resp["data"]
        print(f"   -> Got {len(oi_list)} OI Spurt records.")
        
        # --- DEBUG: PRINT RAW KEYS ---
        if len(oi_list) > 0:
            print(f"   -> [DEBUG] Raw Keys in OI Spurts Data: {list(oi_list[0].keys())}")
            print(f"   -> [DEBUG] Sample Data: {oi_list[0]}")
    else:
        print("   -> WARNING: Could not fetch OI Spurts.")

    # ---------------------------------------------------------
    # STEP 3: UNION MERGE (Robust extraction)
    # ---------------------------------------------------------
    print("3. Merging datasets (Union Strategy)...")
    final_map = {}

    for oi_item in oi_list:
        sym = oi_item.get("symbol")
        if sym:
            # TRY ALL POSSIBLE KEY VARIATIONS
            p_change_oi = get_robust_val(oi_item, [
                "pchangeinOpenInterest", "pChangeInOI", "pChangeInOpenInterest", "percentChange", "pChgInOI"
            ])
            change_oi = get_robust_val(oi_item, [
                "changeinOpenInterest", "changeInOI", "changeInOpenInterest", "chgInOI"
            ])
            open_interest = get_robust_val(oi_item, [
                "openInterest", "futureOpenInterest", "totOI", "totalOpenInterest"
            ])
            last_price = get_robust_val(oi_item, [
                "latestPrice", "lastPrice", "ltp"
            ])

            final_map[sym] = {
                "underlying": sym,
                "symbol": sym,
                "pChangeInOpenInterest": p_change_oi,
                "changeinOpenInterest": change_oi,
                "openInterest": open_interest,
                "lastPrice": last_price,
                "source": "OI_SPURTS"
            }

    # Overlay Master Data
    for sym, master_item in master_map.items():
        if sym in final_map:
            # Preserve the extracted OI values
            p_change_oi = final_map[sym]["pChangeInOpenInterest"]
            change_oi = final_map[sym]["changeinOpenInterest"]
            oi_val = final_map[sym]["openInterest"] # Keep OI from spurts if Master is missing it
            
            final_map[sym].update(master_item)
            
            # Restore OI values if Master overwrote them with 0 or None
            if p_change_oi != 0: final_map[sym]["pChangeInOpenInterest"] = p_change_oi
            if change_oi != 0: final_map[sym]["changeinOpenInterest"] = change_oi
            if oi_val != 0: final_map[sym]["openInterest"] = oi_val
            
            final_map[sym]["source"] = "MERGED"
        else:
            # Use robust extractor on Master items too, just in case
            master_item["pChangeInOpenInterest"] = get_robust_val(master_item, ["pchangeinOpenInterest", "pChangeInOI", "pChange"])
            master_item["changeinOpenInterest"] = get_robust_val(master_item, ["changeinOpenInterest", "changeInOI", "change"])
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
