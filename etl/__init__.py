from etl.factTrade import fact_trade
from etl.factQuote import fact_quote
from etl.factMarketIndex import fact_marketindex
from etl.dimDate import dim_date
from etl.dimExchange import dim_exchange
from etl.dimIndex import dim_index
from etl.dimSession import dim_session
from etl.dimSymbol import dim_symbol
from etl.dimTime import dim_time
from etl.featureML import feature


def main():
    while True:
        print("\n=== MENU ===")
        print("1. Dim Date")
        print("2. Dim Time")
        print("3. Dim Exchange")
        print("4. Dim Index")
        print("5. Dim Session")
        print("6. Dim Symbol")
        print("7. Fact Trade")
        print("8. Fact Quote")
        print("9. Fact Market Index")
        print("10. Feature train Machine Learning")
        print("0. Exit")
        
        value = input("Bazzi super handsome choice please: ").strip()
        
        if value == "1":
            dim_date()
        elif value == "2":
            dim_time()
        elif value == "3":
            dim_exchange()
        elif value == "4":
            dim_index()
        elif value == "5":
            dim_session()
        elif value == "6":
            dim_symbol()
        elif value == "7":
            fact_trade()
        elif value == "8":
            fact_quote()
        elif value == "9":
            fact_marketindex()
        elif value == "10":
            feature()
        elif value == "0":
            print("Bye Bazzi handsome ^^")
            break
        else:
            print("Invalid choice")
            
if __name__ == "__main__":
    main()
            
         
            