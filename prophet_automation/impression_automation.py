import os
import boto3
import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import io


class S3Utils:

    def __init__(self):

        try:
            self.s3_res = boto3.resource('s3')
            logging.info('S3 resource object created '
                         '---------------------------')
        except Exception as e:
            logging.error(e)
            logging.error('Failed to connect to S3 resource '
                          '---------------------------')

    def read_data_from_bucket(self, bucket_name, partner, file_path) -> \
            pd.DataFrame:
        """
        This method is check the execution status from given date.
        This will query to s3 bucket and get the files data
        :param bucket_name:
        :param partner:
        :return: dataframe: execution data
        """
        # S3 object identifier
        partner_data = pd.DataFrame()
        logging.info(
            f'Fetching data for partner {partner} from file path {file_path}')

        bucket = self.s3_res.Bucket(bucket_name)

        try:
            for obj in bucket.objects.all():
                if file_path == obj.key:
                    body = obj.get()['Body'].read()
                    partner_data = pd.read_csv(io.BytesIO(body))
                    logging.info(
                        'Completed reading data from above mentioned path')
        except Exception as e:
            logging.critical(e)
            logging.critical(
                'Failed to get data for partner : ' + partner)
        return partner_data


class Detection:

    def __init__(self, partner_name, parent_bucket):

        self.s3_obj = S3Utils()
        self.partner_name = partner_name
        self.parent_bucket = parent_bucket
        logging.info('Initializing Anomaly detection Done ')

    def detect(self, delta_dict, bulk):
        """
        It will compare the delta snowflake with prediction lower bound
        If snowflake impression less than lower bound its identified as anomaly
        """
        partner_data = {}
        lower_bound = bulk['lower_bound'].values[0]
        sf_imp = delta_dict['snowflake_imp']
        logging.info(f'lower_bound {lower_bound}')
        logging.info(f'delta_snowflake_imp {sf_imp}')

        partner_data = \
            {'utc_date': bulk['utc_date'].values[0],
             'snowflake_imp': delta_dict['snowflake_imp'],
             'lower_bound': bulk['lower_bound'].values[0],
             'previous_yr_snowflake_imp': bulk['previous_yr_snowflake_imp'].values[0],
             'snowflake_YoY_drop': bulk['snowflake_YoY_drop'].values[0],
             'previous_day_snowflake_imp': bulk['previous_day_snowflake_imp'].values[0],
             'snowflake_DoD_drop': bulk['snowflake_DoD_drop'].values[0],
             'athena_imp': delta_dict['athena_imp'],
             'previous_day_athena_imp': bulk['previous_day_athena_imp'].values[0],
             'athena_DoD_drop': bulk['athena_DoD_drop'].values[0]
             }

        lower_drop = (
            (delta_dict['snowflake_imp'] - bulk['lower_bound'].values[0])/delta_dict['snowflake_imp'])*100

        partner_data['diff_athena_snowflake'] = ((partner_data['athena_imp']-partner_data['snowflake_imp'])/partner_data['snowflake_imp'])*100
        partner_data['lower_drop'] = round(lower_drop, 2)
 
        return partner_data

    def run(self):

        logging.info(
            f'Collecting delta data for partner: {self.partner_name }')
        path = 'partners_data_testing' + '/' + \
            self.partner_name + '/' + self.partner_name + '.csv'
        partner_delta = self.s3_obj.read_data_from_bucket(
            self.parent_bucket, self.partner_name, path)

        partner_bulk, anomaly_partner_data = None, {}
        if not partner_delta.empty:

            if partner_bulk is None:
                logging.info(
                    f'Collecting bulk data for partner: {self.partner_name }')
                bulk_path = 'partners_data_testing' + '/' + \
                    self.partner_name + '/' + self.partner_name + '_bulk.csv'
                partner_bulk = self.s3_obj.read_data_from_bucket(
                    self.parent_bucket, self.partner_name, bulk_path)

            for partner_row in partner_delta.itertuples():
                # Convert named tuple to dictionary
                row = partner_row._asdict()

                partner_delta_date = row['utc_date']

                logging.info(
                    f' Captured delta data for date : {partner_delta_date} ')

                logging.info(
                    f'Collecting bulk data for partner: {self.partner_name}')
                bulk_path = 'partners_data_testing' + '/' + \
                    self.partner_name + '/' + self.partner_name + '_bulk.csv'

                partner_bulk_delta_date_record = partner_bulk.loc[
                    partner_bulk['utc_date'] == partner_delta_date]
                partner_bulk_date = partner_bulk_delta_date_record['utc_date'].values[0]

                logging.info(
                    f'Captured bulk data for date : {partner_bulk_date} ')
                current_date = partner_delta_date
                current_date_formatted = datetime.strptime(
                    current_date, '%Y-%m-%d')

                # Calculate previous year
                previous_year = current_date_formatted - relativedelta(years=1)
                str_previous_yr = previous_year.strftime('%Y-%m-%d')

                # Capture previous year imparession count and get the YoY drop percentage for Snowflake
                previous_year_data = partner_bulk.loc[partner_bulk.utc_date == str_previous_yr]
                previous_year_sf = previous_year_data['snowflake_imp'].values[0]
                year_drop = ((row['snowflake_imp'] - previous_year_sf)/previous_year_sf)*100
                partner_bulk_delta_date_record['previous_yr_snowflake_imp'] = previous_year_sf
                partner_bulk_delta_date_record['snowflake_YoY_drop'] = round(year_drop, 2)

                # Calculate previous day
                previous_day = current_date_formatted - relativedelta(days=1)
                str_previous_day = previous_day.strftime('%Y-%m-%d')

                # Capture previous day imparession count and get the DoD drop percentage for Snowflake
                previous_day_data = partner_bulk.loc[partner_bulk.utc_date ==
                                                     str_previous_day]
                previous_day_sf = previous_day_data['snowflake_imp'].values[0]
                day_drop = (
                    (row['snowflake_imp'] - previous_day_sf)/previous_day_sf)*100
                partner_bulk_delta_date_record['previous_day_snowflake_imp'] = previous_day_sf
                partner_bulk_delta_date_record['snowflake_DoD_drop'] = round(
                    day_drop, 2)

                # Capture previous day imparession count and get the DoD drop percentage for Athena
                previous_day_data = partner_bulk.loc[partner_bulk.utc_date == str_previous_day]
                previous_day_at = previous_day_data['athena_imp'].values[0]
                day_drop = ((row['athena_imp'] - previous_day_at)/previous_day_at)*100

                partner_bulk_delta_date_record['previous_day_athena_imp'] = previous_day_at
                partner_bulk_delta_date_record['athena_DoD_drop'] = round(day_drop, 2)

                logging.info('Initializing Anomaly detection ')

                detected_partners_date = self.detect(row, partner_bulk_delta_date_record)

                if detected_partners_date:

                    if self.partner_name in anomaly_partner_data:

                        existing_anomaly_details = anomaly_partner_data[self.partner_name]
                        if existing_anomaly_details:
                            existing_anomaly_details.append(detected_partners_date)
                            anomaly_partner_data[self.partner_name] = existing_anomaly_details
                    else:
                        anomaly_partner_data[self.partner_name] = [detected_partners_date]

        logging.info(
            f'Anomaly detection process completed for {self.partner_name} partner ')
        logging.info(
            f'Captured details for Anomaly partners are {anomaly_partner_data}')
        return anomaly_partner_data


partner = "facebook"
parent_bucket = "iasdl-datalake-raw-ue1-cre-dev"
detection = Detection(partner, parent_bucket)
partner_data = detection.run()
if partner_data:
    print(partner_data)
    # anomaly_partners_data_list.append(anomaly_data)
    # logging.info('Currently captured anomaly details {anomaly_data}')
