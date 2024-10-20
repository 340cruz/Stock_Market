import json
import pandas as pd
from sqlalchemy import create_engine, text


engine = create_engine(f"mssql+pyodbc://localhost\\SQLEXPRESS/STOCK_MARKET?driver=ODBC+Driver+17+for+SQL+Server")
with open("C:\\Users\\legai\\PycharmProjects\\StockMarket\\data\\company_tickers.json") as f:
    data = json.load(f)

json_df = pd.DataFrame.from_dict(data, orient='index')

try:
    # Attempting to read from the database
    sql_df = pd.read_sql('SELECT * FROM stock_market..all_stocks', con=engine)
    # print(df_sql)
except Exception as e:
    print(f"An error occurred: {e}")

json_df['cik_str'] = json_df['cik_str'].astype(str).str.replace('.00000$', '', regex=True).str.zfill(10)
json_df = json_df.rename(columns={'ticker': 'Ticker'})

df_merged = pd.merge(sql_df, json_df, on='Ticker', how='left')

with engine.connect() as connection:  # Open a connection to the database
    with connection.begin():
        for index, row in df_merged.iterrows():
            if pd.notnull(row['cik_str']):  # Check if the CIK from the JSON is not null
                update_query = text(f"""
                           UPDATE stock_market..all_stocks
                           SET CIK = '{row['cik_str']}'
                           WHERE Ticker = '{row['Ticker']}'
                           """)
                # Execute the query with parameters
                # connection.execute(update_query, {'CIK': row['CIK'], 'Ticker': row['ticker']})
                result = connection.execute(update_query)
                # print(f"Rows updated: {result.rowcount}")
                # print(
                #     f"Executing SQL: UPDATE stock_market..all_stocks SET CIK = '{row['cik_str']}' WHERE Ticker = '{row['Ticker']}'")




