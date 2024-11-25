from Scripts.sql_connect import connect_to_stock_db
import pandas as pd
import numpy as np
import json
import os

database = 'STOCK_MARKET'
conn = connect_to_stock_db(database)
cursor = conn.cursor()

json_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'company_tickers_exchange.json')

with open(json_file_path) as f:
    data = json.load(f)

ticker_df = pd.DataFrame(data["data"], columns=data["fields"])
ticker_df['cik'] = ticker_df['cik'].astype(str).str.replace('.00000$', '', regex=True).str.zfill(10)


for index, row in ticker_df.iterrows():
    cursor.execute("INSERT INTO all_stocks (ticker, CIK ,name, Exchange)"
                   " VALUES (?, ?, ?, ?)",
                   row['ticker'], row['cik'], row['name'], row['exchange'])
conn.commit()
print("data inserted")

cursor.close()
conn.close()
