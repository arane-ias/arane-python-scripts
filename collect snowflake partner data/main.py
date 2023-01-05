from check import SnowflakeData
from athena import AthenaQuery
import sys

utc_date = ""
if len(sys.argv) == 2:
    utc_date = sys.argv[1]
else:
    utc_date = input("Enter UTC-date (year-month-day) e.g 2023-01-02 : ")       

snowflake_data = SnowflakeData(utc_date)
qry_sf_dates_df = snowflake_data.get_snowflake_data()
snowflake_data.process_data(qry_sf_dates_df)

utc_date = input("Enter UTC-date (yearmonthday) e.g. 20230102 : ")
athena_data = AthenaQuery()
athena_data.execute_query(utc_date)