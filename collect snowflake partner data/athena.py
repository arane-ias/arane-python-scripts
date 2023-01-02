import boto3
import time
import re
from pathlib import Path

class AthenaQuery:
    def __init__(self) -> None:
        self.client = boto3.client('athena')
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
                        filename = re.findall('.*\/(.*)', s3_path)[0]
                        return filename
                time.sleep(1)
        return False

    def athena_query(self):
        execution = self.client.start_query_execution(
            QueryString = self.query,
            QueryExecutionContext = {
                'Database': 'partner_raw',
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration = {
                'OutputLocation' : 's3://partners_data'
            },
            WorkGroup='cre'
        )
        return execution

    def call(self):
        execution = self.athena_query()
        execution_id = execution['QueryExecutionId']
        filename = self.athena_to_s3(execution_id)
        if (not filename):
            print("Time exceeds ...")
        else:
            print(filename)

data = AthenaQuery()
data.call()
