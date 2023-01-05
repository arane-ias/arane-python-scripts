import boto3
import time
import re
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import botocore
import pandas as pd

class AthenaQuery:
    def __init__(self) -> None:
        # os.system("datalake-cre url")
        self.client = boto3.client('athena','us-east-1')
        self.s3 = boto3.resource('s3')
        self.session = boto3.Session(region_name='us-east-1')
        self.query = Path('athena.sql').read_text()
    
    def athena_to_s3(self,execution_id):
        state = 'RUNNING'
        while (True and state in ['RUNNING', 'QUEUED']):
                response = self.client.get_query_execution(QueryExecutionId = execution_id)

                if 'QueryExecution' in response and \
                        'Status' in response['QueryExecution'] and \
                        'State' in response['QueryExecution']['Status']:
                    state = response['QueryExecution']['Status']['State']
                    if state == 'FAILED':
                        return False
                    elif state == 'SUCCEEDED':
                        s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                        filename = s3_path
                        filename = re.findall('.*\/(.*)', s3_path)[0]
                        return filename
                time.sleep(1)
        return False

    def athena_query(self,dates):
        execution = self.client.start_query_execution(
            QueryString = self.query,
            QueryExecutionContext = {
                'Database': 'partner_raw',
                'Catalog': 'AwsDataCatalog'
            },
            WorkGroup='cre',
            ExecutionParameters=dates
        )
        return execution

    def execute(self,dates):
        execution = self.athena_query(dates)
        execution_id = execution['QueryExecutionId']
        filename = self.athena_to_s3(execution_id)
        if (not filename):
            print("No Data ...")
        else:
            return filename

    def execute_query(self,utc_date):
        self.utc_date = utc_date

        utc_date = datetime.strptime(utc_date,'%Y%m%d')

        prev_utc_date = (utc_date - relativedelta(days=1)).strftime('%Y%m%d')
        self.prev_utc_date = prev_utc_date

        fb_prev_utc_date = (utc_date - relativedelta(days=2)).strftime('%Y%m%d')

        utc_date = utc_date.strftime('%Y%m%d')

        dates = []

        for i in range(5):
            dates.append("'" + utc_date + "'")
            dates.append("'" + prev_utc_date + "'")

        dates.append("'" + prev_utc_date + "'")
        dates.append("'" + fb_prev_utc_date + "'")

        filename = self.execute(dates)
        self.get_csv(filename)
        self.process_data()

    def get_csv(self,filename):
        bucket_name = 'iasqr-cre-784347022195-us-east-1'

        try:
            self.s3.Bucket(bucket_name).download_file(filename, 'athena-results.csv')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def process_data(self):
        athena_df = pd.read_csv('athena-results.csv')
        partner_name = ['Pinterest','Linkedin','Spotify','Snapchat','Facebook','Yahoo']
        for name in partner_name:
            if name not in athena_df['src'].values:
                date = ''
                if name == 'Facebook':
                    date = self.prev_utc_date
                else:
                    date = self.utc_date

                new_row = {'src': name,
                            'utcdate': date,
                            'imps_count': 'NA',
                            'previous_day_count': 'NA',
                            'DOD_percent': 'NA'
                            }
                athena_df = athena_df.append(new_row, ignore_index=True)
        
        athena_df.to_csv('../prophet_automation/athena-results.csv')


# utc_date = ""
# if len(sys.argv) == 2:
#     utc_date = sys.argv[1]
# else:
#     utc_date = input("Enter UTC-date : ")       

# data = AthenaQuery()
# data.execute_query(utc_date)
