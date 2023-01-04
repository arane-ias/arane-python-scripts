import boto3
import time
import re
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import botocore

class AthenaQuery:
    def __init__(self) -> None:
        self.client = boto3.client('athena',)
        self.s3 = boto3.resource('s3')
        self.session = boto3.Session()
        self.query = Path('athena.sql').read_text()
    
    def athena_to_s3(self,execution_id):
        state = 'RUNNING'
        max_execution = 10
        while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
                max_execution = max_execution - 1
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
            WorkGroup='cre'
        )
        return execution

    def call(self,dates):
        execution = self.athena_query(dates)
        print(execution)
        execution_id = execution['QueryExecutionId']
        filename = self.athena_to_s3(execution_id)
        if (not filename):
            print("Time exceeds ...")
        else:
            print(filename)
            return filename

    def execute_query(self,utc_date):
        utc_date = datetime.strptime(utc_date,'%Y-%m-%d')

        prev_utc_date = (utc_date - relativedelta(days=1)).strftime('%Y-%m-%d')

        fb_prev_utc_date = (utc_date - relativedelta(days=2)).strftime('%Y-%m-%d')

        utc_date = utc_date.strftime('%Y-%m-%d')

        dates = []

        for i in range(5):
            dates.append(utc_date)
            dates.append(prev_utc_date)

        dates.append(prev_utc_date)
        dates.append(fb_prev_utc_date)

        filename = self.call(dates)
        print(filename)
        # self.get_csv(filename)

    def get_csv(self,filename):
        bucket_name = 'partners_data'

        try:
            self.s3.Bucket(bucket_name).download_file(filename, 'athena-results.csv')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def set_aws_profile(self):
        os.system("")


utc_date = ""
if len(sys.argv) == 2:
    utc_date = sys.argv[1]
else:
    utc_date = input("Enter UTC-date : ")       

data = AthenaQuery()
data.execute_query(utc_date)
