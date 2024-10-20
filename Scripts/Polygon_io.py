import requests as r
import os
from dotenv import load_dotenv
from polygon import RESTClient
from datetime import datetime, timezone
import pandas as pd
from Scripts.sql_connect import connect_to_stock_db

load_dotenv()

# Variables
api_key = os.getenv("API_KEY")
client = RESTClient(api_key)
database = "STOCK_MARKET"
conn = connect_to_stock_db(database)
cursor = conn.cursor()
table_in_use = "DLY_BASIC_QUOTE"

# User input variables
ticker = input("Enter the stock ticker symbol (e.g., AAPL): ").upper()
start_date = input("Enter the start date (YYYY-MM-DD): ")
end_date = input("Enter the end date (YYYY-MM-DD): ")

# ticker = "AAPL"
# start_date = "2024-05-01"
# end_date = "2024-10-01"

# fetch stock data based on user input and save to a dataframe
aggs = []
for a in client.list_aggs(ticker=ticker, multiplier=1, timespan="day", from_=start_date, to=end_date,
                          limit=50000):
    # print(f"Raw timestamp: {a.timestamp} (in milliseconds)") This was used to debug timestamp
    timestamp_converted = datetime.fromtimestamp(a.timestamp/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    aggs.append({
        "ticker": ticker,
        "rdg_date": timestamp_converted,
        "open": a.open,
        "close": a.close,
        "high": a.high,
        "low": a.low,
        "volume": a.volume,
        "vwap": a.vwap
    })

stock_frame = pd.DataFrame(aggs)
# print(aggs)
# print(stock_frame)


# transmit data from dataframe to sql db/tables
#
for index, row in stock_frame.iterrows():
    cursor.execute(f"INSERT INTO {table_in_use} (TICKER, RDG_DATE, OPEN_PRICE, CLOSE_PRICE, HIGH, LOW, VOLUME, V_WAP)"
                   " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                   row['ticker'], row['rdg_date'], row['open'], row['close'],
                   row['high'], row['low'], row['volume'],row['vwap'])

conn.commit()
cursor.close()
conn.close()





