class EmailReport:
    def __init__(self, impression_details,facebook_missing_data) -> None:
        self.impression_details = impression_details
        self.facebook_missing_data = facebook_missing_data
        self.missing_data_length = str(len(facebook_missing_data))
        self.snowflake_row_count = 0
        self.athena_row_count = 0

    def get_status(self, value):
        status = ""
        if (value != 0 and value != "NA") and value != 'NO CHANGE':
            if value < 0:
                status = "Drop "
            else:
                status = "Rise "
        return status

    # creating snowflake table rows dynamically.
    def get_snowflake_row(self, partner_index):
        snowflake_row = "<tr>"

        for partner, row in self.impression_details[partner_index].items():
            dod_status = self.get_status(row['snowflake_DoD_drop'])
            yoy_status = self.get_status(row['snowflake_YoY_drop'])
    
            if dod_status != "" or yoy_status != "":
                if dod_status == "Drop " or yoy_status == "Drop ":
                    
                    if dod_status == "Drop ":
                        if int(row['snowflake_DoD_drop']) < -20:
                            self.snowflake_row_count += 1
                            snowflake_row += "<td>" + row['utc_date'] + "</td>"
                            snowflake_row += "<td>" + partner + "</td>"
                            if dod_status != "":
                                snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "% " + "</td>"
                            else:
                                snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "</td>"
                            
                            if yoy_status != "":
                                snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "% " + "</td>"
                            else:
                                snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "</td>"
                        elif yoy_status == "Drop ":
                            if int(row['snowflake_YoY_drop']) < -20:
                                self.snowflake_row_count += 1
                                snowflake_row += "<td>" + row['utc_date'] + "</td>"
                                snowflake_row += "<td>" + partner + "</td>"
                                if dod_status != "":
                                    snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "% " + "</td>"
                                else:
                                    snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "</td>"
                                
                                if yoy_status != "":
                                    snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "% " + "</td>"
                                else:
                                    snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "</td>"
                    elif yoy_status == "Drop ":
                        if int(row['snowflake_YoY_drop']) < -20:
                            self.snowflake_row_count += 1
                            snowflake_row += "<td>" + row['utc_date'] + "</td>"
                            snowflake_row += "<td>" + partner + "</td>"
                            if dod_status != "":
                                snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "% " + "</td>"
                            else:
                                snowflake_row += "<td>" + dod_status + str(row['snowflake_DoD_drop']) + "</td>"
                            
                            if yoy_status != "":
                                snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "% " + "</td>"
                            else:
                                snowflake_row += "<td>" + yoy_status + str(row['snowflake_YoY_drop']) + "</td>"
                    

        snowflake_row += "</tr>"
        return snowflake_row

    # creating athena table rows dynamically.
    def get_athena_row(self, partner_index):
        athena_row = "<tr>"

        for partner, row in self.impression_details[partner_index].items():
            
            if partner in ['Snapchat','Facebook','Yahoo']:
                dod_status = self.get_status(row['athena_DoD_drop'])

                if dod_status != "":
                    if dod_status == "Drop ":
                        if int(row['athena_DoD_drop']) < -20:
                            athena_row += "<td>" + row['utc_date'] + "</td>"
                            athena_row += "<td>" + partner + "</td>"
                            athena_row += "<td>" + dod_status + str(row['athena_DoD_drop']) + "% " + "</td>"
                            self.athena_row_count += 1
            
        athena_row += "</tr>"
        return athena_row

    # creating discrepancies table rows dynamically.
    def get_partner_discrepancies_row(self, partner_index):
        discrepancy_row = "<tr>"

        for partner, row in self.impression_details[partner_index].items():
            discrepancy_row += "<td>" + partner + "</td>"
            discrepancy_row += "<td>" + row['utc_date'] + "</td>"
            discrepancy_row += "<td>" + str(row['athena_imp']) + "</td>"
            discrepancy_row += "<td>" + str(row['snowflake_imp']) + "</td>"
            sign = ""
            if str(row['diff_athena_snowflake']) != "NA":
                sign = "% "
            discrepancy_row += "<td>" + self.get_status(row['diff_athena_snowflake']) + str(row['diff_athena_snowflake']) + sign + "</td>"

            sign = ""
            if str(row['athena_DoD_drop']) != "NA":
                sign = "% "
            discrepancy_row += "<td>" + self.get_status(row['athena_DoD_drop']) + str(row['athena_DoD_drop']) + sign + "</td>"

            sign = ""
            if str(row['snowflake_DoD_drop']) != "NA":
                sign = "% "
            discrepancy_row += "<td>" + self.get_status(row['snowflake_DoD_drop']) + str(row['snowflake_DoD_drop']) + sign + "</td>"

            sign = ""
            if str(row['snowflake_YoY_drop']) != "NA":
                sign = "% "
            discrepancy_row += "<td>" + self.get_status(row['snowflake_YoY_drop']) + str(row['snowflake_YoY_drop']) + sign + "</td>"

        discrepancy_row += "</tr>"
        return discrepancy_row

    # facebook missing files table
    def create_fb_files_missing_table(self):
        html_table = ""
        no_of_missing_dates = int(self.missing_data_length)
        cols_count = 9
        for col in range(cols_count):
            html_row = "<tr>"
            if col == 0:
                html_row = "<tr style='color: red;'>"
            for row in range(no_of_missing_dates):
                html_row = html_row + "<td>" + self.facebook_missing_data[row][col] + "</td>"
            html_row += "</tr>"
            
            html_table += html_row
        return html_table

    # creating email template.
    def create_email_template(self):
        html_body = "<!DOCTYPE html><head><style>th {font-size: 18px;}table,th,td {border: 1px solid black;border-collapse: collapse;padding: 5px; text-align: center;} .snowflake {background: #66ccff}.powerbi {background: #CBC3E3}.athena {background: #ccffcc}.diff {background: #d9d9d9}.fb {background: #ffb3b3}</style></head><body>Hello Team,<br/><br/>Please find the attached metrics for Walled Garden partners -<br/><br/>"
        snowflake_table = "<table><th colspan='4' class='snowflake'>Snowflake</th><tr class='snowflake'><td>Date</td><td>Partner Name</td><td>DoD Drop</td><td>YoY Drop</td></tr>"
        athena_table = "<table><th colspan='3' class='athena'>Athena</th><tr class='athena'><td>Date</td><td>Partner Name</td><td>DoD Drop</td></tr>"
        discrepancy_table = "<table><th colspan='8' class='diff'>Discrepancies between Snowflake and partner_raw tables.</th><tr class='diff'><td>PMI</td><td>Date</td><td>partner_raw</td><td>Snowflake</td><td>% dif between Snowflake and partner_raw</td><td>parter_raw DoD % change</td><td>snowflake DoD % change</td><td>snowflake YoY % change</td></tr>"
        missing_files_table = "<table><th colspan='" + self.missing_data_length + "' class='fb'>Facebook Missing Files/hours stats:</th>"

        snowflake_rows = athena_rows = discrepancy_rows = ""
        table_ending_tag = "</table><br /> <br />"
        ending_tag = " Thanks & Regards    <br/> CRE Team</body></html>"
        fb_table_end_tag = "<th colspan='" + self.missing_data_length + "'>Note: The numbers shown above are missing hours</th>"

        for partner_index in range(0, len(self.impression_details)):
            snowflake_rows += self.get_snowflake_row(partner_index)
            athena_rows += self.get_athena_row(partner_index)
            discrepancy_rows += self.get_partner_discrepancies_row(partner_index)

        facebook_missing_files_table = self.create_fb_files_missing_table()

        result = html_body

        if self.snowflake_row_count != 0:
            result = result + snowflake_table + snowflake_rows + table_ending_tag
        if self.athena_row_count != 0:
            result = result + athena_table + athena_rows + table_ending_tag

        result = result + discrepancy_table + discrepancy_rows + table_ending_tag + missing_files_table + facebook_missing_files_table + fb_table_end_tag + table_ending_tag + ending_tag

        f = open("generated_mail_report.html", "w")
        f.write(result)
        f.close()