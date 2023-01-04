import boto3
import time
import re


class AthenaQuery:
    def __init__(self) -> None:
        self.client = boto3.client('athena', 'us-east-1')
        self.session = boto3.Session(region_name='us-east-1')

    def athena_to_s3(self, execution_id):
        state = 'RUNNING'
        max_execution = 10
        while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
            max_execution = max_execution - 1
            response = self.client.get_query_execution(
                QueryExecutionId=execution_id)

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

    def athena_query(self, query):
        execution = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': 'partner_raw',
                'Catalog': 'AwsDataCatalog'
            },
            WorkGroup='cre'
        )
        return execution

    def call(self, query):
        execution = self.athena_query(query)
        print(execution)
        execution_id = execution['QueryExecutionId']
        filename = self.athena_to_s3(execution_id)
        if (not filename):
            print("Time exceeds ...")
        else:
            print(filename)


data = AthenaQuery()
data.call("select 'Snapchat' as src, utcdate , count(distinct impressionid) as imps_count from snapchat  where type = 'impression' and utcdate in ('2023-01-02', '2023-01-01') group by utcdate) order by utcdate desc limit 1")

# import boto3
# import botocore

# bucket_name = 'arane-test-ias'
# key = 'cc3fbbe1-153a-4f2b-af9a-4b0037c090e2.csv'

# s3 = boto3.resource('s3')

# try:
#     s3.Bucket(bucket_name).download_file(key, 'my_local_image.csv')
# except botocore.exceptions.ClientError as e:
#     if e.response['Error']['Code'] == "404":
#         print("The object does not exist.")
#     else:
#         raise
