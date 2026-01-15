import requests
import boto3
import os
import json
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
    "Referer": "https://www.nseindia.com/market-data/equity-derivatives-watch",
}

def create_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        # Visit homepage first to get valid cookies
        s.get("https://www.nseindia.com", timeout=15)
        return s
    except Exception as e:
        print(f"   -> Error initializing session: {e}")
        sys.exit(1)

def fetch_data(session, url):
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def get_robust_val(item, keys_to_try):
    """Helper to find a value from a list of possible key names."""
    for k in keys_to_try:
        if k in item and item[k] is not None:
            try:
                # Remove commas (e.g. "1,200.00" -> 1200.00) and convert
                return float(str(item[k]).replace(',', ''))
            except:
                continue
    return 0.0

def get_merged_nse_data():
    s = create_session()
    
    # ---------------------------------------------------------
    # STEP 1: Fetch Master Price List (Good for LTP/Price)
    # ---------------------------------------------------------
    print("1. Fetching Master List...")
    master_resp = fetch_data(s, "https://www.nseindia.com/api/liveEquity-derivatives?index=stock_fut")
    
    master_map = {}
    if master_resp and "data" in master_resp:
        for item in master_resp["data"]:
            sym = item.get("underlying", "")
            if sym: master_map[sym] = item

    # ---------------------------------------------------------
    # STEP 2: Fetch OI Spurts (Good for Open Interest)
    # ---------------------------------------------------------
    print("2. Fetching OI Spurts...")
    oi_resp = fetch_data(s, "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings")
    
    oi_list = []
    if oi_resp and "data" in oi_resp:
        oi_list = oi_resp["data"]
        # DEBUG: Print keys to see what NSE is doing today
        if len(oi_list) > 0:
            print(f"   -> [DEBUG] Current Keys: {list(oi_list[0].keys())}")

    # ---------------------------------------------------------
    # STEP 3: UNION MERGE & CALCULATION (The Fix)
    # ---------------------------------------------------------
    print("3. Merging & Calculating...")
    final_map = {}

    for oi_item in oi_list:
        sym = oi_item.get("symbol")
        if not sym: continue

        # A. EXTRACT RAW VALUES (Trying all known aliases)
        # -------------------------------------------------
        # Try to find Latest OI
        latest_oi = get_robust_val(oi_item, ["latestOI", "openInterest", "totOI"])
        
        # Try to find Previous OI (Crucial for calculation)
        prev_oi = get_robust_val(oi_item, ["prevOI", "previousOI"])
        
        # Try to find Absolute Change
        change_oi = get_robust_val(oi_item, ["changeInOI", "changeinOpenInterest", "chgInOI"])
        
        # Try to find Price
        last_price = get_robust_val(oi_item, ["underlyingValue", "latestPrice", "lastPrice", "ltp"])

        # B. CALCULATE PERCENTAGE MANUALLY
        # -------------------------------------------------
        # We ignore NSE's 'pChange' field because it often vanishes.
        # Formula: (Change / Previous) * 100
        p_change_oi = 0.0
        
        # If we have 'prevOI', calculate directly
        if prev_oi > 0:
            p_change_oi = (change_oi / prev_oi) * 100
            
        # Fallback: If 'prevOI' is missing but we have 'latest' and 'change'
        # Previous = Latest - Change
        elif latest_oi > 0:
            calculated_prev = latest_oi - change_oi
            if calculated_prev > 0:
                p_change_oi = (change_oi / calculated_prev) * 100

        # C. BUILD RECORD
        # -------------------------------------------------
        final_map[sym] = {
            "underlying": sym,
            "symbol": sym,
            "pChangeInOpenInterest": round(p_change_oi, 2), # <-- The Calculated Value
            "changeinOpenInterest": change_oi,
            "openInterest": latest_oi,
            "lastPrice": last_price,
            "source": "OI_SPURTS"
        }

    # D. OVERLAY MASTER DATA (If available)
    for sym, master_item in master_map.items():
        if sym in final_map:
            # We trust Master Data for PRICE, but trust OI Spurts for OI
            # So we only update Price fields from Master
            m_price = get_robust_val(master_item, ["lastPrice", "ltp"])
            if m_price > 0:
                final_map[sym]["lastPrice"] = m_price
            final_map[sym]["source"] = "MERGED"
        else:
            # If stock only exists in Master (rare), add it
            master_item["pChangeInOpenInterest"] = 0
            final_map[sym] = master_item

    return {"data": list(final_map.values()), "timestamp": datetime.now().isoformat()}

def save_to_dynamodb(json_data):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DDB_TABLE)
        table.put_item(Item={
            "PK": "NSE#OI",
            "SK": "LATEST",
            "updatedAt": datetime.now().isoformat(),
            "data": json.dumps(json_data["data"]) 
        })
        print("   -> Write to DynamoDB Successful!")
    except Exception as e:
        print(f"   -> DynamoDB Write Error: {e}")

if __name__ == "__main__":
    print(f"Starting Scraper...")
    full_data = get_merged_nse_data()
    print(f"   -> Final Count: {len(full_data['data'])}")
    save_to_dynamodb(full_data)
    print("Done.")
