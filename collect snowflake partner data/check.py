import pandas as pd
from pathlib import Path
import snowflake.connector as sf
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# date_input = args.date_input
query = Path('query.sql').read_text()
utc_date = sys.argv[1]

ACCOUNT = os.getenv('ACCOUNT')
WAREHOUSE = os.getenv('WAREHOUSE')
DATABASE = os.getenv('DATABASE')
REGION = os.getenv('AWS_REGION_NAME')
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PWD')

extra = {
    "account": ACCOUNT,
    "warehouse": WAREHOUSE,
    "database": DATABASE,
    "region": REGION
}

print('Collected Snowflake details from airflow connection.....................')
try:
    conn = sf.connect(
    user=USER,
    password=PASSWORD, ## read from file
    account=extra['account'],
    warehouse=extra['warehouse'],
    database=extra['database'],
    schema='ANALYTICS',
    )
    print('Connection has been established with snowflake database.....................')
except Exception as e:
    print(e)
    print('Failed to connect snowflake database.....................')


set_date = "set max_date_1 = (cast('" + utc_date + "' as date));"
try:
    qry_sf_dates_df = pd.read_sql(con=conn, sql=set_date)
    qry_sf_dates_df = pd.read_sql(con=conn, sql=query)
    
    print('Captured the max date with below result')
    print(qry_sf_dates_df)
    qry_sf_dates_df.to_csv('snowflake-resutls.csv')

except Exception as e:
    print(e)
    print('Failed to trigger database with query.....................')