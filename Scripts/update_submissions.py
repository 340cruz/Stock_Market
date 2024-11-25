import json
import numpy as np
import requests as r
import os
import zipfile
from Scripts.sql_connect import connect_to_stock_db
import pandas as pd
from sqlalch_connect_to_db import connect_to_db
import datetime


current_directory = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the script
# current_directory = os.path.dirname(os.path.abspath(__name__))
project_root = os.path.abspath(os.path.join(current_directory, '..'))  # Go up one level
save_directory = os.path.join(project_root, 'data')  # Now access the 'data' directory
database = "STOCK_MARKET"

# """No longer needed since we dont need to unzip"""
# def unzip_submissions_file():
#     zip_file_path = os.path.join(save_directory, 'submissions.zip')
#     extract_to_folder = os.path.join(save_directory, 'submissions')
#
#     os.makedirs(extract_to_folder, exist_ok=True)
#
#     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#         zip_ref.extractall(extract_to_folder)


def download_zip():
    headers = {'User-Agent': 'legair91@gmail.com'}
    url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    filename = "submissions.zip"

    os.makedirs(save_directory, exist_ok=True)

    file_path = os.path.join(save_directory, filename)

    response = r.get(url, headers=headers)

    if response.status_code == 200:
        # Write the content to the file
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded and saved to {file_path}")
    else:
        print(f"Failed to download file: {response.status_code}")


class SubmissionsTableCreation:
    def __init__(self):
        self.connection = None
        self.all_r_df = None
        self.updated_r_df = None
        self.sql_df = None

    def connect(self):
        self.connection = connect_to_stock_db(database)

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

    def create_submissions_table(self):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False
        query = """
        CREATE TABLE [SUBMISSIONS](
        Ticker varchar(15),
        CIK varchar(10),
        ACCN VARCHAR(20),
        FILING_DATE DATE,
        REPORT_DATE DATE,
        FORM VARCHAR(80),
        FILE_NUMBER VARCHAR(30),
        CORE_TYPE VARCHAR(50),
        DOC_DESC VARCHAR(MAX),
        File_name varchar(max),
        [MOD_DATE] datetime,
        PRIMARY KEY (TICKER, ACCN)
        )
        """
        # return self.create_table('SUBMISSIONS', query)
        success = self.create_table('SUBMISSIONS', query)

        if success:  # If table creation is successful
            print("Table created successfully. Now creating the trigger...")
            try:
                self.create_submissions_table_trigger()  # Call the trigger creation function
            except Exception as e:
                print(f"Failed to create trigger: {e}")
        else:
            print("Failed to create table. Trigger creation skipped.")

        return success

    def create_submissions_table_trigger(self):
        if self.connection is None:
            print("No Connection available. Please connect first")
            return False
        query = """
        CREATE TRIGGER last_mod_add_sub
        ON Stock_Market..SUBMISSIONS
        AFTER INSERT 
        AS
            BEGIN
                UPDATE SUB
                    SET MOD_DATE = getdate()
                FROM STOCK_MARKET..SUBMISSIONS AS SUB
                    INNER JOIN inserted as I
                    on I.Ticker = SUB.Ticker
                    and I.ACCN = SUB.ACCN
                END;       
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        print("Trigger successfully created")

    def convert_json_to_df(self):
        json_dir = os.path.join(save_directory, 'submissions.zip')
        # test_file = 'CIK0000002809.json'
        db_connect = connect_to_db()
        # db_connect = self.connection
        query = "select a.Ticker, a.CIK from STOCK_MARKET..ALL_STOCKS a"
        self.sql_df = pd.read_sql(query, db_connect)
        self.sql_df['CIK'] = self.sql_df['CIK'].astype(str)
        print("Tickers imported from sql DB, creating dfs from submission json files..")

        all_records = []
        updated_records = []
        with zipfile.ZipFile(json_dir, 'r') as zip_ref:
            json_files = [f for f in zip_ref.namelist() if f.endswith('json') and f.startswith('CIK')]
            for file_name in json_files:
                if 'submissions' not in file_name:
                    try:
                        with zip_ref.open(file_name) as file:
                            data = json.load(file)
                            cik_number = file_name[3:-5]

                            # if '-' in file_name:
                            #     cik_number = file_name.split('-')[0][3:]  # For filenames like "CIK0000001800-submissions-001.json"
                            # else:
                            #     cik_number = file_name[3:-5]
                            accession_numbers = (
                                    data.get('filings', {}).get('recent', {}).get('accessionNumber', []) or data.get
                            ('accessionNumber', []))
                            filing_date = data.get('filings', {}).get('recent', {}).get('filingDate', []) or data.get(
                                'filingDate', [])
                            report_date = data.get('filings', {}).get('recent', {}).get('reportDate', []) or data.get(
                                'reportDate', [])
                            form = data.get('filings', {}).get('recent', {}).get('form', []) or data.get('form', [])
                            file_number = data.get('filings', {}).get('recent', {}).get('fileNumber', []) or data.get(
                                'fileNumber', [])
                            core_type = data.get('filings', {}).get('recent', {}).get('core_type', []) or data.get('core_type', [])
                            doc_description = data.get('filings', {}).get('recent', {}).get('primaryDocDescription', []) or data.get(
                                'primaryDocDescription', [])
                            file_name = file_name

                            for accn, f_date, r_date, f, f_num, c_type, doc_desc in zip(
                                    accession_numbers, filing_date, report_date, form, file_number, core_type,
                                    doc_description
                            ):
                                all_records.append({
                                    'CIK': cik_number,
                                    'ACCN': accn,
                                    'Filing_date': f_date,
                                    'Report_date': r_date,
                                    'Form': f,
                                    'File_number': f_num,
                                    'Core_type': c_type,
                                    'Doc_desc': doc_desc,
                                    'file_name': file_name
                                })

                    except Exception as e:
                        print(f"Error processing {file_name}: {e}")
            print("Main submissions df created, creating appended submission files now..")

            for file_name in json_files:
                if 'submissions' in file_name:
                    try:
                        with zip_ref.open(file_name) as file:
                            data = json.load(file)
                            cik_number = file_name.split('-')[0][3:]

                            accession_numbers = (
                                    data.get('filings', {}).get('recent', {}).get('accessionNumber', []) or data.get
                            ('accessionNumber', []))
                            filing_date = data.get('filings', {}).get('recent', {}).get('filingDate', []) or data.get(
                                'filingDate', [])
                            report_date = data.get('filings', {}).get('recent', {}).get('reportDate', []) or data.get(
                                'reportDate', [])
                            form = data.get('filings', {}).get('recent', {}).get('form', []) or data.get('form', [])
                            file_number = data.get('filings', {}).get('recent', {}).get('fileNumber', []) or data.get(
                                'fileNumber', [])
                            core_type = data.get('filings', {}).get('recent', {}).get('core_type', []) or data.get('core_type', [])
                            doc_description = data.get('filings', {}).get('recent', {}).get('primaryDocDescription',
                                                                                            []) or data.get(
                                'primaryDocDescription', [])
                            file_name = file_name

                            for accn, f_date, r_date, f, f_num, c_type, doc_desc in zip(
                                    accession_numbers, filing_date, report_date, form, file_number, core_type,
                                    doc_description
                            ):
                                updated_records.append({
                                    'CIK': cik_number,
                                    'ACCN': accn,
                                    'Filing_date': f_date,
                                    'Report_date': r_date,
                                    'Form': f,
                                    'File_number': f_num,
                                    'Core_type': c_type,
                                    'Doc_desc': doc_desc,
                                    'file_name': file_name
                                })

                    except Exception as e:
                        print(f"Error processing {file_name}: {e}")

        self.all_r_df = pd.DataFrame(all_records)
        self.updated_r_df = pd.DataFrame(updated_records)
        print("DFs created")
        # return all_r_df, updated_r_df, sql_df

    def clean_merge_write_dfs(self):
        if self.all_r_df is None or self.updated_r_df is None or self.sql_df is None:
            raise ValueError("Dataframes no initialized. Run convert_json_to_df() first.")
        db_connect = connect_to_db()

        for df in [self.all_r_df, self.updated_r_df]:
            df['CIK'] = df['CIK'].astype(str)
            df['ACCN'] = df['ACCN'].astype(str)
            df['Filing_date'] = pd.to_datetime(df['Filing_date'], errors='coerce').dt.date
            df['Report_date'] = pd.to_datetime(df['Report_date'], errors='coerce').dt.date
            df['Form'] = df['Form'].astype(str)
            df['File_number'] = df['File_number'].astype(str)
            df['Core_type'] = df['Core_type'].astype(str)
            df['Doc_desc'] = df['Doc_desc'].astype(str)
            df['file_name'] = df['file_name'].astype(str)

        print("Removing old submissions..")
        self.updated_r_df = self.updated_r_df[self.updated_r_df['Report_date'] > pd.to_datetime('2014-01-01').date()]
        self.all_r_df = self.all_r_df[self.all_r_df['Report_date'] > pd.to_datetime('2014-01-01').date()]

        # df_merged = pd.merge(all_r_df, updated_r_df, on=['CIK', 'ACCN'], how='left', suffixes=('', '_updated'))
        df_merged = pd.merge(self.all_r_df, self.updated_r_df, on=['CIK', 'ACCN'], how='outer', suffixes=('', '_updated'))
        # df_m2 = df_merged.copy()

        for col in self.updated_r_df.columns:
            if col not in ['CIK', 'ACCN']:  # We don't want to overwrite CIK and ACCN
                # Replace only if the updated value is not NaN and is different from the original
                df_merged[col] = np.where(
                    df_merged[f'{col}_updated'].notna() & (df_merged[f'{col}_updated'] != df_merged[col]),
                    df_merged[f'{col}_updated'],  # Replace with updated value if condition is true
                    df_merged[col]  # Keep the original value if condition is false
                )
        print("Submissions DFs have been merged, cleaning up df now..")
        df_merged.drop(columns=[col for col in df_merged.columns if col.endswith('_updated')], inplace=True)

        df_merged = df_merged[df_merged['Report_date'] > pd.to_datetime('2014-01-01').date()]
        df_merged = df_merged[df_merged['Form'].isin(['10-K', '10-Q', '8-K', '4', '6-K', '10-QT', '10-QT/A', '8-K/A', '10-Q/A'])]
        bad_values = df_merged[df_merged['Report_date'] > pd.to_datetime('11/01/2024').date()].copy()  # identify bad values
        df_merged = df_merged[~df_merged['ACCN'].isin(bad_values['ACCN'])]  # removed the bad values
        bad_values['Report_date'] = bad_values.apply(
            lambda row: datetime.date(
                year=row['Filing_date'].year,
                month=row['Report_date'].month,
                day=row['Report_date'].day
            ) if pd.notnull(row['Filing_date']) and pd.notnull(row['Report_date']) else None,
            axis=1
        )
        bad_values = bad_values[~((bad_values['Form'] == '6-K') & (bad_values['Report_date'] > pd.to_datetime('12/01/2024').date()))]
        df_merged = pd.concat([df_merged, bad_values], ignore_index=True)
        final_merge = pd.merge(self.sql_df, df_merged, on=['CIK'], how='inner')
        print("Final merge complete, moving this to your DB now..")
        final_merge.to_sql("SUBMISSIONS", db_connect, if_exists='append', index=False)
        # db_connect.close() #No longer needed since sqlalchemy closes on its own
        print("Records successfully created")
