import pyodbc as db
from Scripts.sql_connect import connect_to_stock_db

# conn = db.connect(
#     'DRIVER={SQL Server};'
#     'SERVER=localhost\\SQLEXPRESS;'
#     'Trusted_Connection=yes',
#     autocommit=True
# )

database = 'STOCK_MARKET'

conn = connect_to_stock_db(database)

cursor = conn.cursor()
cursor.execute("USE STOCK_MARKET")
cursor.execute("""
CREATE TABLE ALL_STOCKS (
Ticker varchar(15) PRIMARY KEY,
NAME varchar(max),
MARKET_CAP float,
IPO_YEAR varchar(5),
VOLUME float,
Sector varchar(70),
Industry varchar(120)
)
""")
conn.commit()

print("Table created successfully")

conn.close()
