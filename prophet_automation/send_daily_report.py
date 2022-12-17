import boto3
import os
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
source = os.getenv('SES_SENDER')
source_arn = os.getenv('SES_SOURCE_ARN')
from_arn = os.getenv('SES_SOURCE_ARN')
receivers = os.getenv('SES_RECEIVER')

# ses client
ses_client = boto3.client("ses", region_name=AWS_REGION_NAME)

# message body
message = MIMEMultipart()
message['Subject'] = 'Partners Daily Report'
# message['From'] = 'arane@dev.303net.net'
# message['To'] = 'arane@integralads.com'

# html content - report template
f = open('generated_mail_report.html','r')
html_content = f.read()

# Attach the email html content
part = MIMEText(html_content, 'html')
message.attach(part)

# Attach partner daily report excel sheet
part = MIMEApplication(open('final_report.xlsx', 'rb').read())
part.add_header('Content-Disposition', 'attachment', filename='final_report.xlsx')
message.attach(part)

# send mail
response = ses_client.send_raw_email(
    Source=source,
    SourceArn=source_arn,
    FromArn=from_arn,
    Destinations=receivers,
    RawMessage={
        'Data': message.as_string()
    }
)
