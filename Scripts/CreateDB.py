import pyodbc as db
from Scripts.sql_connect import connect_to_stock_db

database = 'STOCK_MARKET'

conn = connect_to_stock_db(database)
# This is to test my connection

#
# cursor = conn.cursor()
#
# cursor.execute("SELECT TOP 10 * FROM Itunes..ALL_SONGS")
#
# for row in cursor:
#     print(row)
#
# conn.close()

# actual code

cursor = conn.cursor()
cursor.execute("create database STOCK_MARKET")
conn.commit()

cursor.execute("USE STOCK_MARKET")
cursor.execute("""
CREATE TABLE ALL_STOCKS (
Ticker varchar(15) PRIMARY KEY,
NAME varchar(max),
MARKET_CAP int,
IPO_YEAR varchar(5),
VOLUME int,
Sector varchar(70),
Industry varchar(120)
)
""")
conn.commit()

print("Table created successfully")

conn.close()

# cursor.execute("USE STOCK_MARKET")

# cursor.execute("DROP DATABASE STOCK_MARKET")
# print("drop successful")
# conn.close()