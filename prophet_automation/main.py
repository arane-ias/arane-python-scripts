from input_data import PartnersData
from report_template import EmailReport
from excel_sheet import ExcelSheet
from facebook_missing_files import FacebookFiles
from send_daily_report import SendMail

snowflake_file = "snowflake-results.csv"
athena_file = "athena-results.csv"
powerbi_file = "powerbi.csv"
utc_date = '2022-12-27'

data = PartnersData(snowflake_file,athena_file,powerbi_file)
impression_details, excel_sheet_data = data.get_partners_data()
print(excel_sheet_data['Youtube - Partner Sold'])
facebook_data = FacebookFiles()
facebook_missing_data = facebook_data.get_facebook_missing_files(utc_date)

report = EmailReport(impression_details,facebook_missing_data)
report.create_email_template()

partner_name = ['Pinterest','Linkedin','Spotify','Snapchat','Twitter','Facebook','Youtube - Google Ads','Youtube - DV 360','Youtube - Partner Sold','Youtube - Reserve','Yahoo']
attributes = ['snowflake_imp','previous_day_snowflake_imp','previous_yr_snowflake_imp','snowflake_DoD_drop','snowflake_YoY_drop','athena_imp','previous_day_athena_imp','athena_DoD_drop','powerbi_imp','previous_yr_powerbi_imp','powerbi_YoY_drop']

sheet = ExcelSheet(partner_name,attributes,excel_sheet_data)
sheet.create_excel_report()

receivers = [['arane@integralads.com'],
        ['arane@integralads.com']]

for receiver in receivers:
    ans = input("Enter your answer (yes/no) : ")
    if ans == "yes" or ans ==  "YES":
        mail = SendMail()
        mail.sendMail(receiver)