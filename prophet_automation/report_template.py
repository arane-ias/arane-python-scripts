

class EmailReport:
    def __init__(self, impression_details) -> None:
        self.impression_details = impression_details

    # creating snowflake table rows dynamically.
    def get_snowflake_row(self,partner_index):
        snowflake_row = "<tr>"

        for partner, records in self.impression_details[partner_index].items():
            for row in records:
                snowflake_row += "<td>" + row['utc_date'] + "</td>"
                snowflake_row += "<td>" + partner + "</td>"
                snowflake_row += "<td>" + str(row['snowflake_DoD_drop']) + "</td>"
                snowflake_row += "<td>" + str(row['snowflake_YoY_drop']) + "</td>"
                snowflake_row += "</tr>"

        snowflake_row += "</tr>"
        return snowflake_row

    # creating athena table rows dynamically.
    def get_athena_row(self,partner_index):
        athena_row = "<tr>"

        for partner, records in self.impression_details[partner_index].items():
            for row in records:
                athena_row += "<td>" + row['utc_date'] + "</td>"
                athena_row += "<td>" + partner + "</td>"
                athena_row += "<td>" + str(row['athena_DoD_drop']) + "</td>"
                athena_row += "<td>" + str(row['athena_YoY_drop']) + "</td>"
                athena_row += "</tr>"

        athena_row += "</tr>"
        return athena_row

    # creating discrepancies table rows dynamically.
    def get_partner_discrepancies_row(self,partner_index):
        discrepancy_row = "<tr>"

        for partner, records in self.impression_details[partner_index].items():
            for row in records:
                discrepancy_row += "<td>" + partner + "</td>"
                for param in row:
                    discrepancy_row += "<td>" + str(row[param]) + "</td>"
                    discrepancy_row += "</tr>"
        discrepancy_row += "</tr>"
        return discrepancy_row

    # creating email template.
    def create_email_template(self):
        html_body = "<!DOCTYPE html><head><style>table,th,td {border: 1px solid black;border-collapse: collapse;padding: 5px;}th {text-align: center;}</style></head><body>Hello Team,<br/><br/>Please find the attached metrics for Walled Garden partners -<br/><br/>"
        
        snowflake_table = "<table><th colspan='4'>Snowflake</th><tr><td>Date</td><td>Partner Name</td><td>DoD Drop</td><td>YoY Drop</td></tr>"
        athena_table = "<table><th colspan='4'>Athena</th><tr><td>Date</td><td>Partner Name</td><td>DoD Drop</td><td>YoY Drop</td></tr>"
        discrepancy_table = "<table><th colspan='9'>Discrepancies between Snowflake and partner_raw tables.</th><tr><td>PMI</td><td>Date</td><td>Snowflake</td><td>partner_raw</td><td>% dif between Snowflake and partner_raw</td><td>Snowflake DoD % change</td><td>snowflake YoY % change</td><td>partner_raw DoD % change</td><td>partner_raw YoY % change</td></tr>"

        snowflake_rows = athena_rows = discrepancy_rows = ""
        table_ending_tag = "</table><br /> <br />"
        ending_tag = " Thanks & Regards    <br/> CRE Team</body></html>"

        for partner_index in range(0,len(self.impression_details)):
            snowflake_rows += self.get_snowflake_row(partner_index)
            athena_rows += self.get_athena_row(partner_index)
            discrepancy_rows += self.get_partner_discrepancies_row(partner_index)

        # combining all email html content & storing it into html file.
        result = html_body + snowflake_table + snowflake_rows + table_ending_tag + athena_table + \
                        athena_rows + table_ending_tag + discrepancy_table + \
                            discrepancy_rows + table_ending_tag + ending_tag
        f = open("generated_mail_report.html", "w")
        f.write(result)
        f.close()


# main
impression_details = [{'spotify':
                       [
                           {'utc_date': '2022-12-06', 'snowflake_imp': 4200496,  'athena_imp': 0, 'diff': 0,
                               'snowflake_DoD_drop': -6.23, 'snowflake_YoY_drop': -53.31, 'athena_DoD_drop': 0, 'athena_YoY_drop': 0},
                           {'utc_date': '2022-12-07', 'snowflake_imp': 3936743, 'athena_imp': 0, 'diff': 0,
                               'snowflake_DoD_drop': -6.28, 'snowflake_YoY_drop': -53.31, 'athena_DoD_drop': 0, 'athena_YoY_drop': 0}
                       ]
                       },
                      {'snapchat':
                       [
                           {'utc_date': '2022-12-06', 'snowflake_imp': 4200496,  'athena_imp': 0, 'diff': 0,
                            'snowflake_DoD_drop': -6.23, 'snowflake_YoY_drop': -53.31, 'athena_DoD_drop': 0, 'athena_YoY_drop': 0},
                           {'utc_date': '2022-12-07', 'snowflake_imp': 3936743, 'athena_imp': 0, 'diff': 0,
                            'snowflake_DoD_drop': -6.28, 'snowflake_YoY_drop': -53.31, 'athena_DoD_drop': 0, 'athena_YoY_drop': 0}
                       ]
                       },]

report = EmailReport(impression_details)
report.create_email_template()                    
