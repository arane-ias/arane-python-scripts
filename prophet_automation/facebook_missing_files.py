import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
 
import boto3

load_dotenv()
 
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
ENV = "prod"
DOWNLOADED_DONE_PATH = os.getenv('DOWNLOADED_DONE_PATH')
FB_FILE_TYPES = [
    "an_init", "an_player", "fb_display", "fb_init", "fb_player", "ig_display", "ig_init", "ig_player"
]

# ENV = "dev"

def path_exists(client, file_path):
    result = client.list_objects(Bucket=f"iasdl-edgeapps-raw-ue1-pmi-{ENV}", Prefix=file_path)
    exists = False
    size = None
    if "Contents" in result:
        exists = True
        size = result["Contents"][0]["Size"]

    return (exists, size)

def create_missing_fb_dict(utc_date,dict):
    my_result = [utc_date]

    for key in dict:
        res = ""
        if dict[key]:
            for hour in dict[key]:
                res += hour + ","
        else:
            res = "All Ok"
            
        my_result.append(res)
    
    return my_result
 
def main():
    utc_date = '2022-12-12'

    dict = { 'an_init': [], 'an_player': [], 'fb_display': [],'fb_init': [],'fb_player': [],'ig_display': [],'ig_init': [],'ig_player': []}

    # check_dynamo_and_fb_api()
    session = boto3.session.Session()
    s3_client = session.client(service_name="s3", region_name=AWS_REGION_NAME)

    date = datetime.strptime(utc_date, "%Y-%m-%d").date()
    end_date_str = date.strftime('%Y-%m-%d %H:00:00')
    date_range = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    date_range_start = date_range
    date_range_end = date_range + timedelta(days=1)
 
    date_current = date_range_start
 
    # print(path_s3)
    while date_current < date_range_end:
        for fb_type in FB_FILE_TYPES:
            path_s3 = DOWNLOADED_DONE_PATH.format(
                type=fb_type,
                date=date_current.strftime("%Y%m%d"),
                hour=date_current.strftime("%H")
            )
            exists, size = path_exists(s3_client, path_s3)
            if exists:
                pass
            else:
                hour = date_current.strftime("%H")
                dict[fb_type].append(hour)
        date_current = date_current + timedelta(hours=1)
    
    my_result = create_missing_fb_dict(utc_date,dict)
    print(my_result)

if __name__ == "__main__":
    main()
