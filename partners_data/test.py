import boto3
from pathlib import Path
import time
import pandas as pd

query = """select 'Snapchat' as src, '20230105' as utcdate, imps_count, previous_day_count,
            round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
            as DOD_percent
            from
            (
                (
                    select sum(imps_count) as imps_count
                    from
                    (
                        select utcdate , count(distinct impressionid) as imps_count from snapchat
                        where utcdate in ('20230105') and sourceid = 10 and
                        utchour
                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                        group by utcdate

                        union

                        select utcdate, count(distinct impressionid) as imps_count from snapchat
                        where type = 'impression' and  utcdate in ('20230106') and sourceid = 10 and
                        utchour
                        in ('00','01','02','03','04')
                        group by utcdate
                    )
                )
                CROSS JOIN
                (
                    select sum(imps_count) as previous_day_count
                    from
                    (
                        select utcdate, count(distinct impressionid) as imps_count from snapchat
                        where utcdate in ('20230104') and sourceid = 10 and
                        utchour
                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                        group by utcdate

                        union

                        select utcdate, count(distinct impressionid) as imps_count from snapchat
                        where type = 'impression' and utcdate in ('20230105') and sourceid = 10 and
                        utchour
                        in ('00','01','02','03','04')
                        group by utcdate
                    )
                )
            )"""

client = boto3.client('athena','us-east-1')

execution = client.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {
                'Database': 'partner_raw',
                'Catalog': 'AwsDataCatalog'
            },
            WorkGroup='primary'
        )

execution_id = execution['QueryExecutionId']

state = 'RUNNING'
while (True and state in ['RUNNING', 'QUEUED']):
    response = client.get_query_execution(QueryExecutionId = execution_id)

    if 'QueryExecution' in response and \
            'Status' in response['QueryExecution'] and \
            'State' in response['QueryExecution']['Status']:
        state = response['QueryExecution']['Status']['State']
        if state == 'FAILED':
            break
        elif state == 'SUCCEEDED':
            results = client.get_query_results(
                            QueryExecutionId=execution_id
                        )

            print(results)
            
    time.sleep(1)

