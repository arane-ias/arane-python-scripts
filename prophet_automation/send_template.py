import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
source = os.getenv('')
source_arn = os.getenv('SES_SENDER')
receivers = os.getenv('SES_RECEIVER')

class SendReport:
    def __init__(self,html_content) -> None:
        self.ses_client = boto3.client("ses", region_name="us-east-1")
        self.html_content = html_content
        self.client = boto3.session.Session().client(service_name='secretsmanager',region_name='us-east-1')
 
    # def get_secrets(self):
    #     secret_value_response = self.client.get_secret_value(
    #             SecretId='secret_name'
    #         )
    #     secrets = json.loads(secret_value_response["SecretString"])
    #     return secrets

    def send(self):
        # secrets = self.get_secrets()

        response = self.ses_client.send_email(
            SourceArn=source_arn,
            Source=source,
            Destination={
                "ToAddresses": receivers
            },
            Message={
                "Body": {
                    'Html': {
                        'Charset': 'UTF-8',
                        'Data': self.html_content,
                    }
                },
                "Subject": {
                    "Charset": "UTF-8",
                    "Data": "Daily Partners Report",
                },
            }   
        )

        print(response)

f = open('generated_mail_report.html','r')
html_content = f.read()
report = SendReport(html_content)
report.send()