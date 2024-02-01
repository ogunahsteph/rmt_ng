import os
import sys
import json

import numpy as np
import pandas as pd

# from airflow.providers.mysql.hooks.mysql import MySqlHook


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utilities.db import *


# remita_hook = MySqlHook(mysql_conn_id='remita_server', schema='remita_staging')


def normalize_data(df):
    bvn = df[['Bvn']]

    bvn.rename(columns={'Bvn': 'bvn_no'}, inplace=True)

    df1 = pd.json_normalize(df['JsonRecordSet'].apply(json.loads))

    df1 = pd.concat([bvn, df1], axis='columns')

    return df1


def get_crb_history(config_path, client_bvn: str) -> pd.DataFrame:
    # df = remita_hook.get_pandas_df(
    #     sql=f"""
    #         select Bvn, JsonRecordSet from (
    #             select Bvn, JsonRecordSet, rank() over (partition by Bvn order by CreatedDate desc) rnk
    #             from remita_staging.CrbHistoricalReport chr where Bvn = '{client_bvn}'
    #         ) rnked where rnk = 1
    #     """
    # )

    config = read_params(config_path)
    project_dir = config['project_dir']
    dwh_credentials = config["db_credentials"]
    prefix = "REMITA"


    logging.warning('Pull crb report ...')
    sql_crb = f"""
        select Bvn, JsonRecordSet from (
                select Bvn, JsonRecordSet, rank() over (partition by Bvn order by CreatedDate desc) rnk
                from remita_staging.CrbHistoricalReport chr where Bvn = '{client_bvn}'
            ) rnked where rnk = 1
        """
    crb = query_dwh(sql_crb, dwh_credentials, prefix, project_dir)

    return crb


def get_credit_score_details(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    sub_string = 'CREDIT_SCORE_SUMMARY'

    credit_cols = [x for x in df.columns if sub_string in x]
    credit_score_details = df[credit_cols]
    credit_score_details.columns = [x[-1] for x in credit_score_details.columns.str.split('.')]
    credit_score_details['bvn_no'] = client_bvn

    credit_score_details.columns = credit_score_details.columns.str.lower()

    return credit_score_details


def classification_by_institution_summaries_function(df: pd.DataFrame) -> pd.DataFrame:
    classification_by_institution_df = df
    classification_by_institution_summaries = classification_by_institution_df.groupby('bvn_no')[
        'institution_type'].nunique().rename('no_of_institution_types').reset_index()

    return classification_by_institution_summaries


def classify_by_institution(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    # classification_by_institution
    classification_by_institution = pd.json_normalize(df['ConsumerHitResponse.BODY.ClassificationInsType'][0])

    classification_by_institution['bvn_no'] = client_bvn

    classification_by_institution = classification_by_institution[['bvn_no', 'AMOUNT_OVERDUE', 'INSTITUTION_TYPE']]

    classification_by_institution.rename(columns={'AMOUNT_OVERDUE': 'amount_overdue_by_institution_type',
                                                  'INSTITUTION_TYPE': 'institution_type'}, inplace=True)

    classification_by_institution['amount_overdue_by_institution_type'] = [float(x.strip().replace(',', '')) for x in
                                                                           classification_by_institution[
                                                                               'amount_overdue_by_institution_type']]

    classification_by_institution = pd.merge(
        classification_by_institution,
        classification_by_institution.groupby('bvn_no')['institution_type'].nunique().rename('no_of_institution_types').reset_index(),
        on='bvn_no'
    )

    return classification_by_institution


def classify_by_product(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    # classification_by_product
    classification_by_product = pd.json_normalize(df['ConsumerHitResponse.BODY.ClassificationProdType'][0])

    classification_by_product['bvn_no'] = client_bvn

    classification_by_product = classification_by_product[['bvn_no', 'AMOUNT_OVERDUE', 'PRODUCT_TYPE']]

    classification_by_product.rename(columns={'AMOUNT_OVERDUE': 'amount_overdue_by_product_type',
                                              'PRODUCT_TYPE': 'product_type'}, inplace=True)

    classification_by_product['amount_overdue_by_product_type'] = [float(x.strip().replace(',', '')) for x in
                                                                   classification_by_product[
                                                                       'amount_overdue_by_product_type']]

    return classification_by_product


def classify_by_facility(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    df1 = df
    classification_by_facility = pd.json_normalize(df1['ConsumerHitResponse.BODY.CreditFacilityHistory24'][0])

    classification_by_facility['bvn_no'] = client_bvn

    classification_by_facility = classification_by_facility[['bvn_no','ACCOUNT_NUMBER','AMOUNT_OVERDUE_CAL','ASSET_CLASSIFICATION_CAL','CURRENT_BALANCE_CAL',
                                                             'DATE_REPORTED','INSTITUTION_NAME','DATE_REPORTED_AGE',
                                                             'LOAN_STATUS','LOAN_TYPE_VALUE','MATURITY_DT','NUM_OF_DAYS_IN_ARREARS_CAL']]

    classification_by_facility['NUM_OF_DAYS_IN_ARREARS_CAL'] = classification_by_facility['NUM_OF_DAYS_IN_ARREARS_CAL'].astype(int)

    classification_by_facility['AMOUNT_OVERDUE_CAL'] = [float(x.strip().replace(',','')) for x in classification_by_facility['AMOUNT_OVERDUE_CAL']]

    classification_by_facility['CURRENT_BALANCE_CAL'] = [float(x.strip().replace(',','')) for x in classification_by_facility['CURRENT_BALANCE_CAL']]


    classification_by_facility.columns  = classification_by_facility.columns.str.lower()

    classification_by_facility.rename(columns = {'amount_overdue_cal' : 'amount_overdue_by_facility'}, inplace = True)

    classification_by_facility = pd.merge(classification_by_facility, classification_by_facility.groupby('bvn_no')['loan_type_value'].nunique().rename('no_of_loan_types').reset_index(), on = 'bvn_no')

    return classification_by_facility


def classification_by_facility_summaries_function(df: pd.DataFrame) -> pd.DataFrame:
    classification_by_facility_summaries = df.groupby('bvn_no').agg(
        total_no_of_running_facilities = pd.NamedAgg('account_number', 'size'),
        total_overdue_amount = pd.NamedAgg('amount_overdue_by_facility', 'sum'),
        maximum_days_in_arrears = pd.NamedAgg('num_of_days_in_arrears_cal', 'max')
    ).reset_index()

    return classification_by_facility_summaries


def get_credit_profile_overview(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    # credit_profile_overview
    credit_profile_overview = pd.json_normalize(df['ConsumerHitResponse.BODY.CreditProfileOverview'][0])

    credit_profile_overview['bvn_no'] = client_bvn

    credit_profile_overview = credit_profile_overview.iloc[0:2, :]

    credit_profile_overview.columns = credit_profile_overview.columns.str.lower()

    return credit_profile_overview


def get_summary_of_performance(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    summary_of_performance = pd.json_normalize(df['ConsumerHitResponse.BODY.SummaryOfPerformance'][0])

    summary_of_performance['bvn_no'] = client_bvn

    summary_of_performance = summary_of_performance[
        ['bvn_no', 'FACILITIES_COUNT', 'INSTITUTION_NAME', 'NONPERFORMING_FACILITY', 'OVERDUE_AMOUNT',
         'PERFORMING_FACILITY']]

    summary_of_performance.columns = summary_of_performance.columns.str.lower()

    summary_of_performance['facilities_count'] = (summary_of_performance['facilities_count'].str.replace(',', '')).astype(float)

    summary_of_performance['overdue_amount'] = (summary_of_performance['overdue_amount'].str.replace(',', '')).astype(float)

    return summary_of_performance


def get_report_details(df: pd.DataFrame, client_bvn: str) -> pd.DataFrame:
    sub_string = 'ReportDetailBVN'

    report_details_cols = [x for x in df.columns if sub_string in x]

    report_detail = df[report_details_cols]

    report_detail.columns = [x[-1] for x in report_detail.columns.str.split('.')]

    report_detail['bvn_no'] = client_bvn

    report_detail.columns = report_detail.columns.str.lower()

    report_detail = report_detail[['bvn_no', 'cir_number', 'report_order_date']]

    report_detail['bvn_no'] = report_detail['bvn_no'].astype(str)

    return report_detail

def get_deposit_money_banks(config_path) -> list[str]:
   

    config = read_params(config_path)
    project_dir = config['project_dir']
    dwh_credentials = config["db_credentials"]
    prefix = "DWH"


    logging.warning('Pull DMBs...')
    sql_crb = f"""
        select instituition_name  from remita.deposit_money_banks 
        """
    DMBs = query_dwh(sql_crb, dwh_credentials, prefix, project_dir)
    
    DMBs_df = pd.DataFrame(DMBs, columns=['instituition_name'])
    
    DMBs_list = DMBs_df['instituition_name'].tolist()


    return DMBs_list


def get_additional_crb_logic(config_path, client_bvn: str, scoring_summaries: pd.DataFrame) -> dict:
    raw_df = get_crb_history(config_path, client_bvn=client_bvn)
    if raw_df.empty:
        return {
            'crb_data_results': pd.DataFrame({
                'bvn_no': [client_bvn], 'total_no_of_facilities': [np.NAN],
                'total_overdue_amount': [np.NAN], 'maximum_days_in_arrears': [np.NAN],
                'total_number_of_institutions': [np.NAN], 'summary_total_overdue_amount': [np.NAN],
                'is_crb_qualified': [True],
                'is_pay_day_crb_qualified': [True],  
            }, index=[0])
        }

#     if raw_df.empty:
        
#         return {
#             'crb_data_results': pd.DataFrame({
#                 'bvn_no': [client_bvn], 'total_no_of_facilities': [np.NAN],
#                 'total_overdue_amount': [np.NAN], 'maximum_days_in_arrears': [np.NAN],
#                 'total_number_of_institutions': [np.NAN], 'summary_total_overdue_amount': [np.NAN], 'is_crb_qualified': [True],
#                 # 'crb_individual_loan_summaries': [np.NAN]
#             }, index=[0])
#         }

    df1 = normalize_data(raw_df)

    credit_score_details = get_credit_score_details(df=df1, client_bvn=client_bvn)
    classification_by_institution = classify_by_institution(df=df1, client_bvn=client_bvn)

    classification_by_institution_summaries = classification_by_institution_summaries_function(
        classification_by_institution)

    classification_by_product = classify_by_product(df=df1, client_bvn=client_bvn)

    classification_by_facility = classify_by_facility(df=df1, client_bvn=client_bvn)

    classification_by_facility_summaries = classification_by_facility_summaries_function(classification_by_facility)

    summary_of_performance = get_summary_of_performance(df=df1, client_bvn=client_bvn)

    report_detail = get_report_details(df=df1, client_bvn=client_bvn)

    final_df = pd.DataFrame({'bvn_no': [client_bvn]})

    final_df = pd.merge(final_df, classification_by_institution_summaries, on='bvn_no')

    final_df = pd.merge(final_df, classification_by_facility_summaries, on='bvn_no')

    final_df['credit_score_details'] = credit_score_details.to_json(orient='records')

    final_df['classification_by_institution'] = classification_by_institution.to_json(orient='records')

    final_df['classification_by_product_type'] = classification_by_product.to_json(orient='records')

    final_df['classification_by_facility'] = classification_by_facility.to_json(orient='records')

    final_df['summary_performance'] = summary_of_performance.to_json(orient='records')
    
    DMBs_list = get_deposit_money_banks(config_path=config_path)
    is_in_DMBs_list = classification_by_facility['institution_name'].isin(DMBs_list).any()

    
   

    final_df = pd.merge(final_df, report_detail, on='bvn_no')

    final_df['is_crb_qualified'] = False
    # final_df['is_pay_day_crb_qualified'] = False

    final_df.loc[(final_df['total_no_of_running_facilities'] <= 5) &
                 (final_df['maximum_days_in_arrears'] < 60) &
                #  (final_df['total_overdue_amount'] < scoring_summaries['average_salary_payment_last_9_months']) &
                 (final_df['total_overdue_amount'] < scoring_summaries['average_monthly_salary_last_6_months']) &
                #  (final_df['total_overdue_amount'] < scoring_summaries[f'average_monthly_salary_last_{time_period}_months']) &
                 (final_df['no_of_institution_types'] <= 2), 'is_crb_qualified'] = True
    
    final_df['is_pay_day_crb_qualified'] = False
    final_df.loc[(final_df['total_no_of_running_facilities'] <= 5) &
                 (final_df['maximum_days_in_arrears'] < 90) &
                 (final_df['total_overdue_amount'] < scoring_summaries['average_monthly_salary_last_6_months']) &
                 (final_df['no_of_institution_types'] <= 3) & is_in_DMBs_list, 'is_pay_day_crb_qualified'] = True

    final_df.rename(columns={
        'no_of_institution_types': 'crb_no_of_institution_types',
        'total_no_of_running_facilities': 'crb_total_no_of_running_facilities',
        'total_overdue_amount': 'crb_total_overdue_amount',
        'maximum_days_in_arrears': 'crb_maximum_days_in_arrears',
        'credit_score_details': 'crb_credit_score_details',
        'classification_by_institution': 'crb_classification_by_institution',
        'classification_by_product_type': 'crb_classification_by_product_type',
        'classification_by_facility': 'crb_classification_by_facility',
        'summary_performance': 'crb_summary_performance', 'cir_number': 'crb_cir_number',
        'report_order_date': 'crb_report_order_date'
    }, inplace=True)

    return {'crb_data_results': final_df}