import pandas as pd
from pathlib import Path
import snowflake.connector as sf
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# date_input = args.date_input
class SnowflakeData:
    def __init__(self,utc_date) -> None:
        self.query = Path('query.sql').read_text()
        self.utc_date = utc_date
        utc_date = datetime.strptime(utc_date, '%Y-%m-%d')
        self.prev_utc_date = (utc_date - relativedelta(days=1)).strftime('%Y-%m-%d')

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
        twitter_date = "set max_date_2 = (cast('" + self.prev_utc_date + "' as date));"
        try:
            qry_sf_dates_df = pd.read_sql(con=self.conn, sql=set_date)
            qry_sf_dates_df = pd.read_sql(con=self.conn, sql=twitter_date)
            qry_sf_dates_df = pd.read_sql(con=self.conn, sql=self.query)
            
            print('Captured the max date with below result')
            print(qry_sf_dates_df)

        except Exception as e:
            print(e)
            print('Failed to trigger database with query.....................')
        
        return qry_sf_dates_df

    def process_data(self,qry_sf_dates_df):
        partner_name = ['Pinterest','Linkedin','Spotify','Snapchat','Twitter','Facebook','Youtube - Google Ads','Youtube - DV 360','Youtube - Partner Sold','Youtube - Reserve','Yahoo']
        for name in partner_name:
            if name not in qry_sf_dates_df['MEASUREMENT_SOURCE_ID'].values:
                date = ''
                if name == 'Twitter':
                    date = self.prev_utc_date
                else:
                    date = self.utc_date
                new_row = {'MEASUREMENT_SOURCE_ID': name,
                            'HIT_DATE': date,
                            'TOTAL_IMPS': 'NA',
                            'PREVIOUS_IMPS': 'NA',
                            'PREVIOUS_YR_IMPS': 'NA',
                            'DOD_PERCENT': 'NA',
                            'YOY': 'NA'
                            }
                qry_sf_dates_df = qry_sf_dates_df.append(new_row, ignore_index=True)
        
        qry_sf_dates_df.to_csv('../prophet_automation/snowflake-results.csv')
        

utc_date = ""
if len(sys.argv) == 2:
    utc_date = sys.argv[1]
else:
    utc_date = input("Enter UTC-date : ")       

data = SnowflakeData(utc_date)
result = data.get_snowflake_data()
data.process_data(result)