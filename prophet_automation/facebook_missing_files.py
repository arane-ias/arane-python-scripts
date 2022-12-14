import logging
import os
import sys
from datetime import datetime, timedelta, timezone
 
import boto3
 
AWS_REGION_NAME = "us-east-1"
ENV = "prod"
DOWNLOADED_DONE_PATH = "partner_raw/facebook/done/downloaded/{type}/utcdate={date}/utchour={hour}/"
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

    # date_range_start = datetime.strptime("2022-03-17 00:00:00", "%Y-%m-%d %H:%M:%S")
    # date_range_end = datetime.strptime("2022-03-18 23:00:00", "%Y-%m-%d %H:%M:%S")
    dateFormatString = '|{:^21}|'
    formatString = '{:^11}|'
 
    header = ""
    header += dateFormatString.format('datetime UTC')
    for fb_type in FB_FILE_TYPES:
        header += formatString.format(fb_type)
    print(header)
    date_current = date_range_start
 
    # print(path_s3)
    while date_current < date_range_end:
        line_str = dateFormatString.format(date_current.strftime('%Y-%m-%d %H:%M:%S'))
        for fb_type in FB_FILE_TYPES:
            path_s3 = DOWNLOADED_DONE_PATH.format(
                type=fb_type,
                date=date_current.strftime("%Y%m%d"),
                hour=date_current.strftime("%H")
            )
            exists, size = path_exists(s3_client, path_s3)
            if exists:
                if size > 0:
                    line_str += formatString.format('OK')
                else:
                    line_str += formatString.format(f'OK-{size:,}')
            else:
                line_str += formatString.format('NO DATA')
                hour = date_current.strftime("%H")
                dict[fb_type].append(hour)
        date_current = date_current + timedelta(hours=1)
        # print(line_str)
    
    print(dict)

    my_result = [utc_date]

    for key in dict:
        res = ""
        if dict[key]:
            for hour in dict[key]:
                res += hour + ","
        else:
            res = "All Ok"
            
        my_result.append(res)

    print(my_result)

if __name__ == "__main__":
    main()
