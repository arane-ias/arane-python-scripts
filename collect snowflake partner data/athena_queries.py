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
from queries import Queries

class AthenaQuery:
    def __init__(self) -> None:
        os.system("saml2aws login --skip-prompt --role=arn:aws:iam::784347022195:role/DataLake-CRE --force")
        self.client = boto3.client('athena','us-east-1')
        self.s3 = boto3.resource('s3')
        self.session = boto3.Session(region_name='us-east-1')
        self.queries = Queries()
    
    def athena_to_s3(self,execution_id):
        state = 'RUNNING'
        while (True and state in ['RUNNING', 'QUEUED']):
                response = self.client.get_query_execution(QueryExecutionId = execution_id)

                if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                    state = response['QueryExecution']['Status']['State']
                    if state == 'FAILED':
                        return False
                    elif state == 'SUCCEEDED':
                        filename = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                        filename = re.findall('.*\/(.*)', filename)[0]
                        return filename
                time.sleep(1)
        return False

    def athena_query(self,query,dates):
        execution = self.client.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {
                'Database': 'partner_raw',
                'Catalog': 'AwsDataCatalog'
            },
            WorkGroup='primary',
            ExecutionParameters=dates
        )
        return execution['QueryExecutionId']

    def execute(self,query,dates):
        execution_id = self.athena_query(query,dates)
        filename = self.athena_to_s3(execution_id)
        if (not filename):
            return False
        else:
            return filename

    def get_csv(self,partner_name,filename):
        bucket_name = 'aws-athena-query-results-972380794107-us-east-1'

        try:
            self.s3.Bucket(bucket_name).download_file(filename, partner_name+".csv")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def process_data(self):
        athena_df = pd.DataFrame()
        athena_df = athena_df.append(pd.read_csv('Snapchat.csv'), ignore_index=True)
        athena_df = athena_df.append(pd.read_csv('Spotify.csv'), ignore_index=True)
        athena_df = athena_df.append(pd.read_csv('Pinterest.csv'), ignore_index=True)
        athena_df = athena_df.append(pd.read_csv('Yahoo.csv'), ignore_index=True)
        athena_df = athena_df.append(pd.read_csv('Facebook.csv'), ignore_index=True)
        athena_df = athena_df.append(pd.read_csv('Linkedin.csv'), ignore_index=True)
        
        athena_df.to_csv('../prophet_automation/athena-results.csv')

    def execute_query(self,utc_date,partner,query):
        utc_date = datetime.strptime(utc_date,'%Y%m%d')
        prev_utc_date = "'" + (utc_date - relativedelta(days=1)).strftime('%Y%m%d') + "'" 
        fwd_date = "'" + (utc_date + relativedelta(days=1)).strftime('%Y%m%d') + "'"

        utc_date = "'" + utc_date.strftime('%Y%m%d') + "'"

        dates = [utc_date,utc_date,fwd_date,prev_utc_date,utc_date]
        
        filename = self.execute(query,dates)
        if(filename == False):
            print("Manually Enter Data For Partner " + partner)
            date = ''
            if partner == 'Facebook':
                date = self.prev_utc_date
            else:
                date = self.utc_date

            new_row = [{'src': partner, 'utcdate': date, 'imps_count': 'NA', 'previous_day_count': 'NA', 'DOD_percent': 'NA' }]
            df = pd.DataFrame.from_dict(new_row)
            df.to_csv(partner+".csv" , header=True)
        else:
            self.get_csv(partner,filename)

    def partner_queries(self,utc_date):
        prev_utc_date = ((datetime.strptime(utc_date,'%Y%m%d')) - relativedelta(days=1)).strftime('%Y%m%d')
        self.utc_date = utc_date
        self.prev_utc_date = prev_utc_date

        print("Snapchat Query Execution - ")
        self.execute_query(utc_date,"Snapchat",self.queries.snapchat)
        print("Spotify Query Execution - ")
        self.execute_query(utc_date,"Spotify",self.queries.spotify)
        print("Pinterest Query Execution - ")
        self.execute_query(utc_date,"Pinterest",self.queries.pinterest)
        print("Linkedin Query Execution - ")
        self.execute_query(utc_date,"Linkedin",self.queries.linkedin)
        print("Yahoo Query Execution - ")
        self.execute_query(utc_date,"Yahoo",self.queries.yahoo)
        print("Facebook Query Execution - ")
        
        self.execute_query(prev_utc_date,"Facebook",self.queries.fb)
        self.process_data()

# utc_date = ""
# if len(sys.argv) == 2:
#     utc_date = sys.argv[1]
# else:
#     utc_date = input("Enter UTC-date : ")       

# data = AthenaQuery()
# data.partner_queries(utc_date)
