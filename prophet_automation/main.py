from input_data import PartnersData
from report_template import EmailReport
from excel_sheet import ExcelSheet
from facebook_missing_files import FacebookFiles
from send_daily_report import SendMail
from datetime import datetime
from dateutil.relativedelta import relativedelta

snowflake_file = "snowflake-results.csv"
athena_file = "athena-results.csv"
powerbi_file = "powerbi.csv"

utc_date = input("Enter previous utc date for facebbok missing files : ")
prev_utc_date = (datetime.strptime(utc_date,'%Y-%m-%d') - relativedelta(days=1)).strftime('%Y-%m-%d')
utc_dates = [utc_date, prev_utc_date]

data = PartnersData(snowflake_file,athena_file,powerbi_file)
impression_details, excel_sheet_data = data.get_partners_data()

print("Facebook Missing Files Start")

facebook_data = FacebookFiles()
facebook_missing_data = facebook_data.get_fb_missing_files(utc_dates)
facebook_data.delete_aws_profile()

print("Facebook Missing Files Done")

print("Email Body Start")

report = EmailReport(impression_details,facebook_missing_data)
report.create_email_template()

print("Email Body Done")

partner_name = ['Pinterest','Linkedin','Spotify','Snapchat','Twitter','Facebook','Youtube - Google Ads','Youtube - DV 360','Youtube - Partner Sold','Youtube - Reserve','Yahoo']
attributes = ['snowflake_imp','previous_day_snowflake_imp','previous_yr_snowflake_imp','snowflake_DoD_drop','snowflake_YoY_drop','athena_imp','previous_day_athena_imp','athena_DoD_drop','powerbi_imp','previous_yr_powerbi_imp','powerbi_YoY_drop']

print("Excel Report Start")

sheet = ExcelSheet(partner_name,attributes,excel_sheet_data)
filename = sheet.create_excel_report()

print("Excel Report Done")

receivers = [['arane@integralads.com'],
        ['arane@integralads.com']]

mail = SendMail(filename)

print("Send Mail")

for receiver in receivers:
    ans = input("Enter your answer (yes/no) : ")
    if ans == "yes" or ans ==  "YES":
        mail.sendMail(receiver)

mail.delete_aws_profile()