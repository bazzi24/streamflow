import code
import os
import pandas as pd
from datetime import datetime
from ssi_fc_data import fc_md_client, model
import psycopg2
import json
from dataclasses import asdict, is_dataclass, fields
import inspect
import dataclasses
from ssi_fc_data.model.model import daily_stock_price
from psycopg2.extras import execute_batch
import uuid
import traceback; traceback.print_exc()
import logging
from dotenv import load_dotenv

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
    """Save the data to CSV with prefix and timestamp."""
    if not data:
        print("No data to save")
        return
    
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("dataList") or data.get("data") or []
        if isinstance(rows, dict) and "dataList" in rows:
            rows = rows["dataList"]
    else:
        print("The data is not in the correct format (not a list or a dictionary).")
        return
    if not rows:
        print("Response has no dataList")
        return
    
    df = pd.DataFrame(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{ts}.csv"
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} line to {filename}")

def connect_db():
    """ PostgreSQL"""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),  
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST")
    )

def save_to_securities_psql(data, table_name):
    
    if not data:  
        print("There is no data to push to the database.")
        return
    
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        if table_name == "corporation.corporation":
            for item in data:  
                cur.execute(
                    """
                    INSERT INTO corporation.corporation (symbol_id, symbol_name, symbol_en_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol_id) DO NOTHING
                    """,
                    (item.get('Symbol'), item.get('StockName'), item.get('StockEnName'))
                )
        conn.commit()
        print(f"The {len(data)} row has been pushed into {table_name}.")
    except Exception as e:
        print(f"Error when pushing in {table_name}: {e}")
    finally:
        if conn:
            conn.close()
    
def save_to_securities_details_psql(data, table_name):
    if not data:
        print("There is no data to push to the database.")
        return
    
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        if table_name == "corporation.corporation_detail":
            for item in data:
                cur.execute(
                    """
                    INSERT INTO corporation.corporation_detail (symbol_id, detail_json)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol_id) DO NOTHING
                    """,
                    (item.get('Symbol'), json.dumps(item))
                )
        conn.commit()
        print(f"The {len(data)} line has been pushed in {table_name}")
    except Exception as e:
        print(f"Error when pushing into {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def md_get_securities_list():
    req = model.securities("HOSE",1,100)
    resp = client.securities(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    #save_to_csv(resp, "securities_list")
    save_to_securities_psql(resp, "corporation.corporation")
    
def md_get_all_securities():
    markets = ["HOSE", "HNX", "UPCOM"]
    all_rows = []

    for m in markets:
        page = 1
        while True:
            req = model.securities(m, page, 100)
            resp = client.securities(config, req)

            rows = resp.get("data")
            if rows is None:  
                print(f"Market={m}, Page={page}, no data (data=None)")
                break

            print(f"Market={m}, Page={page}, Number of symbols obtained {len(rows)}")

            if not rows:  
                break

            all_rows.extend(rows)
            page += 1

        print(f"Total number of symbols obtained from {m}: {len(all_rows)}")

    if not all_rows:
        print("No data to save")
        return

    save_to_securities_psql(all_rows, "corporation.corporation")
    print(f"Đã lưu {len(all_rows)} Stock from HOSE, HNX, and UPCOM are added to the database.")

def md_get_securities_details():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""SELECT symbol_id
                   FROM corporation.corporation;""")
    
    symbols = [row[0] for row in cursor.fetchall()][:5] 
    all_data = []
    
    #print("Symbols from DB:", symbols)
    
    ALL_MARKETS = ["HOSE", "HNX", "UPCOM"]
    
    for symbol in symbols:
        symbol = symbol.strip()
        print(f"Processing symbol details: '{symbol}'")

        req = model.securities_details(ALL_MARKETS, symbol, 1, 10)
        resp = client.securities_details(vars(config), req)
        data = resp.get("RepeatedInfo") or resp.get("dataList") or resp.get("data") or resp.get("datalist") or []
        print("Full response data:", data)
        if not data:
            print(f"No detailed data available for {symbol}, ignore.")
            continue
        
        all_data.extend(data)
        print(f"Details have been retrieved for {symbol}: {len(data)} line")
        print("Sample data:", data)
    
    '''
    for item in symbols:
        req = model.securities_details(markets, symbols, 1, 10)
        resp = client.securities_details(vars(config), req)
        data = resp.get("RepeatedInfo") or resp.get("data") or []
        print("=== Details ===", data[:3])
    
    '''

    
    
    
    #save_to_csv(resp, "securities_details")
    # Thêm logic cho corporation.corporation_detail nếu cần
    
#def md_get_all_securities_details():
    
    

def md_get_index_components():
    req = model.index_components("VN30", 1, 100)
    resp = client.index_components(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "index_components")
    # Thêm logic cho market.indexComponent nếu cần


# ĐÃ HOÀN THÀNH
def md_get_index_list():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT market_id, market_name FROM market.market;")
    markets = cursor.fetchall()
    print("Markets from DB:", markets)
    #conn.close()    
    all_data = []
    market_map = {name.strip(): mid for mid, name in markets}

    
    
    for market_id, market_name in markets:
        #market_id = market_id.strip()
        market_name = market_name.strip()
        print(repr(market_id), repr(market_name))
        
        req = model.index_list(market_name, pageIndex=1, pageSize=100)
        resp = client.index_list(vars(config), req)
        data = resp.get("dataList") or resp.get("data") or []
        #market_id = market_map.get(market_name)
        try:
            for item in data:
                exchange = item["Exchange"].strip()
                index_code = (item.get("IndexCode") or "").strip()
                index_name = (item.get("IndexName") or "").strip()
                market_id = market_map.get(exchange)
                
                if not index_code:
                    print(f"index_code not found for Exchange {exchange}")
                    continue
                
                if not market_id:
                    print(f"market_id not found for exchange {exchange}")
                    continue
                
                
                
                indexlist_id = index_code or str(uuid.uuid4())  
                index_name = index_name or index_code
                
                all_data.append((indexlist_id, market_id, index_name, index_code))
            print(f"The {len(data)} line has been retrieved for the market. {market_name}")
            
        except Exception as e:
            print(f"Error retrieving market data {market_name}: {e}")
            continue
        finally:
            print(f"The data collection process for the market has ended. {market_name}")
            continue
    
    sql = """
    INSERT INTO market.indexlist (indexlist_id, market_id, index_name, index_code)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (index_code) DO UPDATE
    SET market_id = EXCLUDED.market_id,
        index_name = EXCLUDED.index_name,
        index_code = EXCLUDED.index_code;
    """
    execute_batch(cursor, sql, all_data)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Pushed the {len(all_data)} row into market.index_list.")
    #print("=== Preview ===", resp.get("dataList", [])[:3])
    #save_to_csv(resp, "index_list")
    
    
    

def md_get_daily_OHLC():
    req = model.daily_ohlc("SSI", "01/09/2025", "10/09/2025", 1, 100)
    resp = client.daily_ohlc(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "daily_ohlc")
    

# IT WILL BE PROCESSED VIA STREAMING, SO DO NOT USE IT.
def md_get_intraday_OHLC():
    req = model.intraday_ohlc(symbol="SSI", from_date="10/09/2025", to_date="10/10/2025", page=1, size=100, adjusted=True, interval=5)
    resp = client.intraday_ohlc(config, req)
    print("=== Preview ===", resp.get("dataList", [])[:3])
    save_to_csv(resp, "intraday_ohlc")
    


def md_get_daily_index():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT TRIM(index_code), TRIM(indexlist_id) FROM market.indexlist;")
    indexes = [row[0] for row in cursor.fetchall()]
    print("Indexes from DB:", indexes)
    all_data = []
    
    #index_map = {code.strip(): mid for mid, code, in indexes}
    
    for index_code in indexes:
        #indexlist_id = indexlist_id.strip()
        index_code = index_code.strip()
        print(f"Ma API {repr(index_code)}")

        req = model.daily_index(index_code, index_code, "01/09/2025", "01/10/2025", 1, 100,"", "")
        resp = client.daily_index(vars(config), req)
        data = resp.get("dataList") or resp.get("data") or []
        
        print(data)
        #save_to_csv(data, f"daily_index_{index_code}")
        
        try:
            for item in data:
                
                index_code = (item.get("IndexId") or "").strip()
                #indexlist_id = index_map.get(index_code_api)
                #print("Ma API:", index_code)
                #print("Found:", indexlist_id)
                trading_date_raw = item.get("TradingDate" or "").strip() 
                trading_date = None
                
                if trading_date_raw:
                    try:
                        trading_date = datetime.strptime(trading_date_raw, "%d/%m/%Y").date()
                    except ValueError:
                        print(f"Invalid date: {trading_date_raw}")
                        continue
                    
                time = item.get("Time" or "")
                change = item.get("Change" or "") 
                ratio_change = item.get("RatioChange" or "") 
                total_trade = item.get("TotalTrade" or "") 
                total_match_vol = item.get("TotalMatchVol" or "") 
                total_match_val = item.get("TotalMatchVal" or "") 
                type_index = item.get("TypeIndex" or "")
                index_name = item.get("IndexName" or "").strip() 
                advances = item.get("Advances" or "") 
                no_changes = item.get("NoChanges" or "") 
                declines = item.get("Declines" or "") 
                ceilings = item.get("Ceilings" or "") 
                floors = item.get("Floors" or "") 
                total_deal_vol = item.get("TotalDealVol" or "") 
                total_deal_val = item.get("TotalDealVal" or "") 
                total_vol = item.get("TotalVol" or "") 
                total_val = item.get("TotalVal" or "") 
                trading_session = item.get("TradingSession" or "").strip() 
                indexlist_id = index_code
                if not indexlist_id:
                    print(f"No indexlist_id found for index_code {index_code}")
                    continue
                
                all_data.append((indexlist_id, trading_date, time, change, ratio_change, 
                                 total_trade, total_match_vol, total_match_val,
                                 type_index, index_name, advances, no_changes, declines, 
                                 ceilings, floors, total_deal_vol, total_deal_val, total_vol, 
                                 total_val, trading_session))
                
                #print(f"Retrieved {len(data)} rows for index:", index_code)
        
        
        except Exception as e:
            print(f"Error retrieving data for the index {index_code}: {e}")
            
            print("The item causing the error:", item)
            continue
        finally:
            print(f"The data collection process for the index is now complete {index_code}")
            continue
        
    sql = """
    INSERT INTO market.dailyindex (indexlist_id, trading_date, time, change, ratio_change, 
                                    total_trade, total_match_vol, total_match_val,
                                   type_index, index_name, advances, no_changes, declines, 
                                   ceilings, floors, total_deal_vol, total_deal_val, 
                                   total_vol, total_val, trading_session)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    
    """
    
    execute_batch(cursor, sql, all_data)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"The {len(all_data)} row has been pushed into market.daily_index.")
     
    #req = model.daily_index(index="VNINDEX", exchange="VNINDEX", from_date="01/09/2025", to_date="10/09/2025", page=1, size=100, from_time="", to_time="")
    #resp = client.daily_index(config, req)
    #print("=== Preview ===", resp.get("dataList", [])[:3])
    #save_to_csv(resp, "daily_index")
    # Thêm logic cho market.dailyIndex nếu cần

'''
def kaka():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol_id FROM corporation.corporation;")
    symbols = cursor.fetchall()
    conn.close()
    return symbols
'''


def md_get_stock_price():

    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol_id FROM corporation.corporation;")
    symbols = [row[0] for row in cursor.fetchall()]
    #print(symbols[:3])
    #symbols = symbols[:20]
    #print(model.daily_stock_price)
    
    all_data = []

    for symbol in symbols:
        #print("Convert white space in symbol")
        symbol = symbol.strip()
        #print(repr(symbol))
        #print(f"Processing symbol: '{symbol}'")
        
        req = model.daily_stock_price(symbol, "20/09/2025", "05/10/2025", 1, 100, "")
        resp = client.daily_stock_price(vars(config), req)
        data = resp.get("dataList") or resp.get("data") or []
        
        if not data:
            print(f"No data for {symbol}, ignore.")
            continue


        
        print(data)
        
        try:
            for item in data:
                symbol_id = (item.get("Symbol") or "").strip()
                trading_date_raw = item.get("TradingDate" or "").strip()
                trading_date = None
                if trading_date_raw:
                    try:
                        trading_date = datetime.strptime(trading_date_raw, "%d/%m/%Y").date()
                    except ValueError:
                        print(f"Invalid date: {trading_date_raw}")
                        continue
                price_change = item.get("PriceChange" or "")
                per_price_change = item.get("PerPriceChange" or "")
                ceiling_price = item.get("CeilingPrice" or "")
                floor_price = item.get("FloorPrice" or "")
                ref_price = item.get("RefPrice" or "")
                open_price = item.get("OpenPrice" or "")
                highest_price = item.get("HighestPrice" or "")
                lowest_price = item.get("LowestPrice" or "")
                close_price = item.get("ClosePrice" or "")
                average_price = item.get("AveragePrice" or "")
                close_price_adjusted = item.get("ClosePriceAdjusted" or "")
                total_match_vol = item.get("TotalMatchVol" or "")
                total_match_val = item.get("TotalMatchVal" or "")
                total_deal_val = item.get("TotalDealVal" or "")
                total_deal_vol = item.get("TotalDealVol" or "")
                foreign_buy_vol_total = item.get("ForeignBuyVolTotal" or "")
                foreign_current_room = item.get("ForeignCurrentRoom" or "")
                foreign_sell_vol_total = item.get("ForeignSellVolTotal" or "")
                foreign_buy_val_total = item.get("ForeignBuyValTotal" or "")
                foreign_sell_val_total = item.get("ForeignSellValTotal" or "")
                total_buy_trade = item.get("TotalBuyTrade" or "")
                total_buy_trade_vol = item.get("TotalBuyTradeVol" or "")
                total_sell_trade = item.get("TotalSellTrade" or "")
                total_sell_trade_vol = item.get("TotalSellTradeVol" or "")
                net_buy_sell_vol = item.get("NetBuySellVol" or "")
                net_buy_sell_val = item.get("NetBuySellVal" or "")
                total_traded_vol = item.get("TotalTradedVol" or "")
                total_traded_value = item.get("TotalTradedValue")
                #symbol_id = item.get("Symbol" or "").strip()
                time = item.get("Time" or "")
                #symbol_id = item.get("Symbol" or "").strip()
                
                
                
                all_data.append((symbol_id, trading_date, price_change, per_price_change,
                                ceiling_price, floor_price, ref_price, open_price,
                                highest_price, lowest_price, close_price, average_price,
                                close_price_adjusted, total_match_vol, total_match_val,
                                total_deal_val, total_deal_vol, foreign_buy_vol_total,
                                foreign_current_room, foreign_sell_vol_total, foreign_buy_val_total,
                                foreign_sell_val_total, total_buy_trade, total_buy_trade_vol,
                                total_sell_trade, total_sell_trade_vol, net_buy_sell_vol,
                                net_buy_sell_val, total_traded_vol, total_traded_value, time))
                
                #print(f"Data o day: {all_data}")
                
                #print(f"Đã lấy {len(data)} dòng cho mã", symbol_id)
                
          
        except Exception as e:
            print(f"Error retrieving data")   
            print("Error item: ", e)
            traceback.print_exc() 
            continue
        finally:
            print(f"The data collection process for the index has ended. {symbol_id}")
            
    sql = """
    INSERT INTO ohlc.dailystockprice (symbol_id, trading_date, price_change, per_price_change,
                                    ceiling_price, floor_price, ref_price, open_price,
                                    highest_price, lowest_price, close_price, average_price,
                                    close_price_adjusted, total_match_vol, total_match_val,
                                    total_deal_val, total_deal_vol, foreign_buy_vol_total,
                                    foreign_current_room, foreign_sell_vol_total, foreign_buy_val_total,
                                    foreign_sell_val_total, total_buy_trade, total_buy_trade_vol,
                                    total_sell_trade, total_sell_trade_vol, net_buy_sell_vol,
                                    net_buy_sell_val, total_traded_vol, total_traded_value, time)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        
    """
    
    execute_batch(cursor, sql, all_data)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Saved {len(all_data)} rows to ohlc.dailystockprice")
    

'''def md_get_stock_price_intraday():
    sorted'''
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
        print("19 - Load All Securities to DB")
        print("0  - Exit")

        value = input("Enter your choice: ").strip()

        if value == "11":
            md_get_all_securities()
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
            print("Bye")
            break
        else:
            print("Invalid choice!")

if __name__ == "__main__":
    main()