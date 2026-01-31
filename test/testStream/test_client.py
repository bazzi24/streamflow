import os
import pandas as pd
from datetime import datetime
from ssi_fc_data import fc_md_client, model
from dotenv import load_dotenv
# import config

load_dotenv()

config = {
    "auth_type": os.getenv("auth_type"),
    "consumerID": os.getenv("consumerID"),
    "consumerSecret": os.getenv("consumerSecret"),
    "url": os.getenv("url"),
    "stream_url": os.getenv("stream_url")
}

client = fc_md_client.MarketDataClient(config)

# ==== Helper ====
def save_to_csv(data, prefix):
    """Lưu dữ liệu ra CSV với prefix + timestamp"""
    if not data:
        print("Không có dữ liệu để lưu")
        return
    # Nhiều API trả dataList
    rows = data.get("dataList") or data.get("data") or []
    if isinstance(rows, dict) and "dataList" in rows:
        rows = rows["dataList"]
    if not rows:
        print("Response không có dataList")
        return
    df = pd.DataFrame(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{ts}.csv"
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"Đã lưu {len(df)} dòng vào {filename}")

# ==== API wrappers ====
def md_get_securities_list():
    req = model.securities("HOSE", 1, 100)
    resp = client.securities(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])  # in 3 dòng đầu
    save_to_csv(resp, "securities_list")

def md_get_securities_details():
    req = model.securities_details(market=["HOSE", "HNX"], pageIndex=1, pageSize=10)
    resp = client.securities_details(config, req)
    print("=== Details ===", resp)
    save_to_csv(resp, "securities_details")

def md_get_index_components():
    req = model.index_components("VN30", 1, 100)
    resp = client.index_components(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "index_components")

def md_get_index_list():
    req = model.index_list("HOSE", 1, 100)
    resp = client.index_list(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "index_list")

def md_get_daily_OHLC():
    req = model.daily_ohlc("SSI", "01/09/2025", "10/09/2025", 1, 100, True)
    resp = client.daily_ohlc(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "daily_ohlc")

def md_get_intraday_OHLC():
    req = model.intraday_ohlc("SSI", "02/10/2025", "03/10/2025", 1, 100, True, 5)
    resp = client.intraday_ohlc(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "intraday_ohlc")

def md_get_daily_index():
    req = model.daily_index("VNINDEX", "VNINDEX", "01/09/2025", "10/09/2025", 1, 100, "", "")
    resp = client.daily_index(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "daily_index")

def md_get_stock_price():
    #conn = connect_db()
    req = model.daily_stock_price("SSI", "01/09/2025", "10/09/2025", 1, 100, "HOSE")
    resp = client.daily_stock_price(config, req)
    print(resp)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "daily_stock_price")

def md_get_all_securities():
    
    markets = ["HOSE", "HNX", "UPCOM"]
    all_rows = []

    for m in markets:
        page = 1
        while True:
            req = model.securities(m, page, 100)
            resp = client.securities(config, req)

            rows = resp.get("data")
            if rows is None:   # ⚡ Fix: nếu API trả về None
                print(f"Market={m}, Page={page}, không có dữ liệu (data=None)")
                break

            print(f"Market={m}, Page={page}, Số mã lấy được={len(rows)}")

            if not rows:   # nếu là list rỗng thì dừng
                break

            all_rows.extend(rows)
            page += 1

        print(f"Tổng số mã lấy được từ {m}: {len(all_rows)}")

    if not all_rows:
        print("Không có dữ liệu để lưu")
        return

    df = pd.DataFrame(all_rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"all_securities_{ts}.csv"
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"Đã lưu {len(df)} mã chứng khoán HOSE + HNX vào {filename}")


# ==== Menu ====
def main():
    while True:
        print("\n=== MENU ===")
        print("11 - Securities List")
        print("12 - Securities Details")
        print("13 - Index Components")
        print("14 - Index List")
        print("15 - Daily OHLC")
        print("16 - Intraday OHLC")
        print("17 - Daily Index")
        print("18 - Stock Price")
        print("19 - All Securities (HOSE + HNX + UPCOM)")
        print("0  - Exit")

        value = input("Enter your choice: ").strip()

        if value == "11":
            md_get_securities_list()
        elif value == "12":
            md_get_securities_details()
        elif value == "13":
            md_get_index_components()
        elif value == "14":
            md_get_index_list()
        elif value == "15":
            md_get_daily_OHLC()
        elif value == "16":
            md_get_intraday_OHLC()
        elif value == "17":
            md_get_daily_index()
        elif value == "18":
            md_get_stock_price()
        elif value == "19":
            md_get_all_securities()
        elif value == "0":
            break
        else:
            print("Invalid choice!")

if __name__ == "__main__":
    main()
