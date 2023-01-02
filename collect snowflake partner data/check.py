import pandas as pd
from pathlib import Path
import snowflake.connector as sf
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# date_input = args.date_input
class SnowflakeData:
    def __init__(self,utc_date) -> None:
        self.query = Path('query.sql').read_text()
        self.utc_date = utc_date

        ACCOUNT = os.getenv('ACCOUNT')
        WAREHOUSE = os.getenv('WAREHOUSE')
        DATABASE = os.getenv('DATABASE')
        REGION = os.getenv('AWS_REGION_NAME')
        USER = os.getenv('SNOWFLAKE_USER')
        PASSWORD = os.getenv('SNOWFLAKE_PWD')

        print('Collected Snowflake details from airflow connection.....................')
        try:
            self.conn = sf.connect(
                    user=USER,
                    password=PASSWORD, ## read from file
                    account=ACCOUNT,
                    warehouse=WAREHOUSE,
                    database=DATABASE,
                    schema='ANALYTICS',
                )
            print('Connection has been established with snowflake database.....................')
        except Exception as e:
            print(e)
            print('Failed to connect snowflake database.....................')

    def get_snowflake_data(self):
        set_date = "set max_date_1 = (cast('" + self.utc_date + "' as date));"
        try:
            qry_sf_dates_df = pd.read_sql(con=self.conn, sql=set_date)
            qry_sf_dates_df = pd.read_sql(con=self.conn, sql=self.query)
            
            print('Captured the max date with below result')
            print(qry_sf_dates_df)
            qry_sf_dates_df.to_csv('../prophet_automation/snowflake-results.csv')

        except Exception as e:
            print(e)
            print('Failed to trigger database with query.....................')

utc_date = ""
if sys.argv[1]:
    utc_date = sys.argv[1]
else:
    utc_date = input("Enter UTC-date : ")       

data = SnowflakeData(utc_date)
data.get_snowflake_data()