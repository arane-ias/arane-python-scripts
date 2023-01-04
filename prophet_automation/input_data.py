import pandas as pd

class PartnersData:
    def __init__(self,snowflake_file,athena_file,powerbi_csv) -> None:
        self.df = pd.read_csv(snowflake_file)
        self.df2 = pd.read_csv(athena_file)
        self.df3 = pd.read_csv(powerbi_csv)

        self.df = self.df.fillna("NA")
        self.df2 = self.df2.fillna("NA")
        self.df3 = self.df3.fillna("NA")

    # 1. remove drop and rise value from dod, it should be no only.
    def get_partners_data(self):
        result = []
        excel_sheet_data = {}
        for index in self.df.index:
            data = {}
            key = self.df['MEASUREMENT_SOURCE_ID'][index]
            data['utc_date'] = self.df['HIT_DATE'][index]
            data['snowflake_imp'] = self.df['TOTAL_IMPS'][index]
            data['snowflake_DoD_drop'] = self.df['DOD_PERCENT'][index]
            data['snowflake_YoY_drop'] = self.df['YOY'][index]
            data['previous_day_snowflake_imp'] = self.df['PREVIOUS_IMPS'][index]
            data['previous_yr_snowflake_imp'] = self.df['PREVIOUS_YR_IMPS'][index]

            if key in ['Pinterest','Linkedin','Spotify','Snapchat','Facebook','Yahoo']:
                for row_index in self.df2.index:
                    if key.lower() == self.df2['src'][row_index].lower():
                        data['athena_imp'] = self.df2['imps_count'][row_index]
                        data['athena_DoD_drop'] = self.df2['DOD_percent'][row_index]
                        data['previous_day_athena_imp'] = self.df2['previous_day_count'][row_index]

                        if data['athena_imp'] == 'NA' or data['snowflake_imp'] == 'NA':
                            data['diff_athena_snowflake'] = 'NA'
                        else:
                            diff_athena_snowflake = round(((data['athena_imp'] - data['snowflake_imp']) / data['snowflake_imp']) * 100)
                            data['diff_athena_snowflake'] = diff_athena_snowflake
                        break
            else:
                data['athena_imp'] = data['athena_DoD_drop'] = data['previous_day_athena_imp'] = data['diff_athena_snowflake'] = 'NA'

            if key in ['Linkedin','Spotify','Snapchat','Twitter']:
                for row_index in self.df3.index:
                    if key.lower() == self.df3['src'][row_index].lower():
                        data['powerbi_imp'] = self.df3['imps_count'][row_index]
                        data['previous_yr_powerbi_imp'] = self.df3['prev_yr_imp'][row_index]
                        data['powerbi_YoY_drop'] = self.df3['yoy_drop'][row_index]
                        break
            else:
                data['powerbi_imp'] = data['previous_yr_powerbi_imp'] = data['powerbi_YoY_drop' ] = 'NA'

            partners_data = {}
            partners_data[key] = data
            excel_sheet_data[key] = data
            result.append(partners_data)

        return result,excel_sheet_data

# snowflake_file = "snowflake-results.csv"
# athena_file = "athena-results.csv"
# powerbi_file = "powerbi.csv"
# obj = PartnersData(snowflake_file,athena_file,powerbi_file)
# obj.get_data()


