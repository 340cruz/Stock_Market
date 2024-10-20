from Scripts.sql_connect import connect_to_stock_db
import pandas as pd
import numpy as np

database = 'STOCK_MARKET'
conn = connect_to_stock_db(database)
cursor = conn.cursor()

df = pd.read_excel("C:\\Users\\legai\\documents\\all_stocks.xlsx", sheet_name='Full')
df['IPOYear'] = df['IPOYear'].astype('Int64').astype(str).replace('<NA>', None)
df['Sector'] = df['Sector'].replace({np.nan: None})
df['Industry'] = df['Industry'].replace({np.nan: None})

for index, row in df.iterrows():
    cursor.execute("INSERT INTO all_stocks (ticker, name, IPO_Year, Sector, Industry)"
                   " VALUES (?, ?, ?, ?, ?)",
                   row['Symbol'], row['Name'], row['IPOYear'], row['Sector'],
                   row['Industry'])
conn.commit()


cursor.execute("""
ALTER TABLE all_stocks
ADD CIK VARCHAR(10) NULL
""")
conn.commit()

cursor.close()
conn.close()
