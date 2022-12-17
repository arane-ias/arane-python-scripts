

class EmailReport:
    def __init__(self, impression_details,facebook_missing_data) -> None:
        self.impression_details = impression_details
        self.facebook_missing_data = facebook_missing_data
        self.missing_data_length = str(len(facebook_missing_data))

    def get_status(self, value):
        status = ""
        if value != 0 or value != "NA":
            if value < 0:
                status = "Drop     "
            else:
                status = "Rise     "
        return status

    # creating snowflake table rows dynamically.
    def get_snowflake_row(self, partner_index):
        snowflake_row = "<tr>"

        for partner, records in self.impression_details[partner_index].items():
            for row in records:
                dod_status = self.get_status(row['snowflake_DoD_drop'])
                yoy_status = self.get_status(row['snowflake_YoY_drop'])
                snowflake_row += "<td>" + row['utc_date'] + "</td>"
                snowflake_row += "<td>" + partner + "</td>"
                snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "</td>"
                snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "</td>"
                snowflake_row += "</tr>"

        snowflake_row += "</tr>"
        return snowflake_row

    # creating athena table rows dynamically.
    def get_athena_row(self, partner_index):
        athena_row = "<tr>"

        for partner, records in self.impression_details[partner_index].items():
            for row in records:
                dod_status = self.get_status(row['athena_DoD_drop'])
                yoy_status = self.get_status(row['athena_YoY_drop'])
                athena_row += "<td>" + row['utc_date'] + "</td>"
                athena_row += "<td>" + partner + "</td>"
                athena_row += "<td>" + dod_status + str(row['athena_DoD_drop']) + "</td>"
                athena_row += "<td>" + yoy_status + str(row['athena_YoY_drop']) + "</td>"
                athena_row += "</tr>"

        athena_row += "</tr>"
        return athena_row

    # creating discrepancies table rows dynamically.
    def get_partner_discrepancies_row(self, partner_index):
        discrepancy_row = "<tr>"

        for partner, records in self.impression_details[partner_index].items():
            for row in records:
                discrepancy_row += "<td>" + partner + "</td>"
                for param in row:
                    status = ""
                    if param in ["diff_athena_snowflake", "snowflake_DoD_drop", "snowflake_YoY_drop", "athena_DoD_drop", "athena_YoY_drop"]:
                        status = self.get_status(row[param])
                    
                    discrepancy_row += "<td>" + status + str(row[param]) + "</td>"
                discrepancy_row += "</tr>"
        discrepancy_row += "</tr>"
        return discrepancy_row

    # facebook missing files table
    def create_fb_files_missing_table(self):
        html_table = ""
        no_of_missing_dates = int(self.missing_data_length)
        cols_count = 9
        for col in range(cols_count):
            html_row = "<tr>"
            for row in range(no_of_missing_dates):
                html_row = html_row + "<td>" + self.facebook_missing_data[row][col] + "</td>"
            html_row += "</tr>"
            
            html_table += html_row
        return html_table

    # creating email template.
    def create_email_template(self):
        html_body = "<!DOCTYPE html><head><style>table,th,td {border: 1px solid black;border-collapse: collapse;padding: 5px;}th {text-align: center;}</style></head><body>Hello Team,<br/><br/>Please find the attached metrics for Walled Garden partners -<br/><br/>"

        snowflake_table = "<table><th colspan='4'>Snowflake</th><tr><td>Date</td><td>Partner Name</td><td>DoD Drop</td><td>YoY Drop</td></tr>"
        athena_table = "<table><th colspan='4'>Athena</th><tr><td>Date</td><td>Partner Name</td><td>DoD Drop</td><td>YoY Drop</td></tr>"
        discrepancy_table = "<table><th colspan='9'>Discrepancies between Snowflake and partner_raw tables.</th><tr><td>PMI</td><td>Date</td><td>partner_raw</td><td>Snowflake</td><td>% dif between Snowflake and partner_raw</td><td>parter_raw DoD % change</td><td>partner_raw YoY % change</td><td>snowflake DoD % change</td><td>snowflake YoY % change</td></tr>"
        missing_files_table = "<table><th colspan='" + self.missing_data_length + "'>Facebook Missing Files/hours stats:</th>"

        snowflake_rows = athena_rows = discrepancy_rows = ""
        table_ending_tag = "</table><br /> <br />"
        ending_tag = " Thanks & Regards    <br/> CRE Team</body></html>"
        fb_table_end_tag = "<th colspan='" + self.missing_data_length + "'>Note: The numbers shown above are missing hours</th>"

        for partner_index in range(0, len(self.impression_details)):
            snowflake_rows += self.get_snowflake_row(partner_index)
            athena_rows += self.get_athena_row(partner_index)
            discrepancy_rows += self.get_partner_discrepancies_row(
                partner_index)

        facebook_missing_files_table = self.create_fb_files_missing_table()

        # combining all email html content & storing it into html file.
        result = html_body + snowflake_table + snowflake_rows + table_ending_tag + athena_table + \
            athena_rows + table_ending_tag + discrepancy_table + \
            discrepancy_rows + table_ending_tag + missing_files_table + facebook_missing_files_table + fb_table_end_tag + table_ending_tag + ending_tag
        f = open("generated_mail_report.html", "w")
        f.write(result)
        f.close()


# main
impression_details = [{'spotify':
                       [
                           {
                               "utc_date": "2022-12-06",
                               "athena_imp": "NA",
                               "snowflake_imp": 3698740759,
                               "diff_athena_snowflake": -100.0,
                               "athena_DoD_drop": "NA",
                               "athena_YoY_drop": "NA",
                               "snowflake_DoD_drop": -1.21,
                               "snowflake_YoY_drop": 18.29
                           }]

                       },
                      {'snapchat':
                       [
                           {
                               "utc_date": "2022-12-06",
                               "athena_imp": "NA",
                               "snowflake_imp": 3698740759,
                               "diff_athena_snowflake": -100.0,
                               "athena_DoD_drop": "NA",
                               "athena_YoY_drop": "NA",
                               "snowflake_DoD_drop": -1.21,
                               "snowflake_YoY_drop": 18.29
                           }
                       ]
                       },]

facebook_missing_data = [
    ['Data Files/Date', 'an_init','an_player','fb_display','fb_init','fb_player','ig_display','ig_init','ig_player'],
    ['2022-12-12','All Ok', 'All Ok', 'All Ok', '18,22,23,', 'All Ok', 'All Ok', '20,21,', 'All Ok'],
    ['2022-12-11','All Ok', 'All Ok', 'All Ok', '18,22,23,', 'All Ok', 'All Ok', '20,21,', 'All Ok']
]

report = EmailReport(impression_details,facebook_missing_data)
report.create_email_template()
