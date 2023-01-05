import re

s3_path = "aws-athena-query-results-972380794107-us-east-1/arane/000b5061-bd3e-45e8-bc50-a98695a8bdef.csv"

filename = re.findall('.*\/(.*)', s3_path)[0]

print(filename)