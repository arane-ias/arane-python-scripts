import boto3
import os
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

class SendMail:
    def __init__(self,filename) -> None:
        os.system("saml2aws login --skip-prompt --role=arn:aws:iam::972380794107:role/IAS-Engineering --force")
        self.AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
        self.source = os.getenv('SES_SENDER')
        self.source_arn = os.getenv('SES_SOURCE_ARN')
        self.from_arn = os.getenv('SES_SOURCE_ARN')
        self.filename = filename

        # ses client
        self.ses_client = boto3.client("ses", region_name=self.AWS_REGION_NAME)

    def delete_aws_profile(self):
        os.system("rm ~/.aws/credentials")

    def sendMail(self, receivers):
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
        part = MIMEApplication(open(self.filename, 'rb').read())
        part.add_header('Content-Disposition', 'attachment', filename=self.filename)
        message.attach(part)

        # send mail
        response = self.ses_client.send_raw_email(
            Source=self.source,
            SourceArn=self.source_arn,
            FromArn=self.from_arn,
            Destinations=receivers,
            RawMessage={
                'Data': message.as_string()
            }
        )
        return response
