from Scripts.sql_connect import connect_to_stock_db
import pyodbc as db


class StockTableConfig:
    def __init__(self, database):
        self.connection = None
        self.database = database

    def connect(self):
        self.connection = connect_to_stock_db(self.database)

    def _table_exists(self, table_name):
        if self.connection is None:
            print("No Connection, please connect")
            return False

        query = f"""
        SELECT CASE
            WHEN OBJECT_ID('STOCK_MARKET..{table_name}' , 'U') IS NOT NULL
            THEN 1 ELSE 0 END
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] == 1
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    def create_table(self, table_name, create_query):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False

        if self._table_exists(table_name):
            print(f" Table {table_name} already exists.")
            return False

        return self._execute_query(create_query)

    def create_daily_table(self):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False
        query = """
        CREATE TABLE DLY_QUOTE(
        TICKER VARCHAR(10),
        RDG_DATE DATE,
        OPEN_PRICE DECIMAL (25,2),
        CLOSE_PRICE DECIMAL (25,2),
        PREV_CLOSE_PRICE DECIMAL (25,2),
        VOLUME BIGINT,
        BID_VALUE BIGINT,
        BID_MULTIPLIER INT,
        ASK_VALUE BIGINT,
        ASK_MULTIPLIER INT,
        SPREAD BIGINT,
        PRIMARY KEY (TICKER, RDG_DATE)
        )
        """
        return self.create_table('DLY_QUOTE', query)

    def create_daily_basic_table(self):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False
        query = """
        CREATE TABLE DLY_BASIC_QUOTE(
        TICKER VARCHAR(10),
        RDG_DATE DATE,
        OPEN_PRICE DECIMAL (25,2),
        CLOSE_PRICE DECIMAL (25,2),
        HIGH DECIMAL (25,2),
        LOW DECIMAL (25,2),
        VOLUME BIGINT,
        V_WAP BIGINT,
        [MOD_DATE] DATETIME,
        PRIMARY KEY (TICKER, RDG_DATE)
        )
        """
        return self.create_table('DLY_BASIC_QUOTE', query)

    def create_minute_table(self):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False
        query = """
        CREATE TABLE MIN_QUOTE(
        TICKER VARCHAR(10),
        RDG_DATE DATETIME,
        PRICE DECIMAL (25,2),
        BID_VALUE BIGINT,
        BID_MULTIPLIER INT,
        ASK_VALUE BIGINT,
        ASK_MULTIPLIER INT,
       [HIGH] DECIMAL (25,2),
        [LOW] DECIMAL (25,2),
        [OPEN] DECIMAL (25,2),
        [CLOSE] DECIMAL (25,2),
        SPREAD BIGINT,
        VOLUME BIGINT,
        TRADE_COUNT BIGINT,
        PRIMARY KEY (TICKER, RDG_TIME)
        )
        """
        return self.create_table('MIN_QUOTE', query)

    def create_15minute_table(self):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False
        query = """
        CREATE TABLE [15_MIN_QUOTE](
        TICKER VARCHAR(10),
        RDG_TIME DATETIME,
        PRICE DECIMAL (25,2),
        BID_VALUE BIGINT,
        BID_MULTIPLIER INT,
        ASK_VALUE BIGINT,
        ASK_MULTIPLIER INT,
        [HIGH] DECIMAL (25,2),
        [LOW] DECIMAL (25,2),
        [OPEN] DECIMAL (25,2),
        [CLOSE] DECIMAL (25,2),
        SPREAD BIGINT,
        VOLUME BIGINT,
        TRADE_COUNT BIGINT,
        PRIMARY KEY (TICKER, RDG_TIME)
        )
        """
        return self.create_table('15_MIN_QUOTE', query)

    def delete_table(self, table_name):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False

        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist, cannot delete")
            return False

        query = f"DROP TABLE [{table_name}]"
        return self._execute_query(query)

    def _execute_query(self, query):
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            print("Table created successfully.")
            return True
        except Exception as e:
            print(f"Error executing query: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()
            print('Connection closed.')
