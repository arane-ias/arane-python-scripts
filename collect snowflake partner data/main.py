from check import SnowflakeData
from athena import AthenaQuery
import sys

utc_date = ""
if len(sys.argv) == 2:
    utc_date = sys.argv[1]
else:
    utc_date = input("Enter UTC-date : ")       

snowflake_data = SnowflakeData(utc_date)
snowflake_data.get_snowflake_data()

athena_data = AthenaQuery()

# athena_data.set_aws_profile()

athena_data.execute_query(utc_date)