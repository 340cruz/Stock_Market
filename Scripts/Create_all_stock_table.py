import pyodbc as db
from Scripts.sql_connect import connect_to_stock_db

database = 'STOCK_MARKET'

conn = connect_to_stock_db(database)

cursor = conn.cursor()
cursor.execute("USE STOCK_MARKET")
cursor.execute("""
CREATE TABLE ALL_STOCKS (
Ticker varchar(15),
CIK varchar(10),
NAME varchar(max),
IPO_YEAR varchar(4),
Sector varchar(70),
Industry varchar(120),
Exchange varchar(10),
PRIMARY KEY (TICKER)
)
""")
conn.commit()

print("Table created successfully")
cursor.close()
conn.close()
