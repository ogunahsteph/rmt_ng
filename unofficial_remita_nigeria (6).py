import os
import sys
import math
import logging
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from pandas.tseries.offsets import MonthEnd
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.data.crb_data_incorporation import *


# remita_hook = MySqlHook(mysql_conn_id='remita_server', database='remita_staging')
# mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-pronto')
# warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")


def calculate_last_9_months_salaries(df):
    count = df['count_of_payments_last_9_months']

    if count >= 6 :
        return True
    else:
        return False


def round_off(n):
    """
    This function rounds off elements by setting a ceiling to the next 100
    """
    return int(math.ceil(n / 100.0)) * 100


def amounts_cap(n):
    """
    This function sets elements caps in line with product terms i.e NGN 100,000 - 1,000,000:
    * merchants qualifying for less than 100K get a 0 loan limit
    * merchants qualifying for more than 1M get the limits back-tracked to 1M
    """
    if n < 30000:
        return 0
    elif n > 1000000:
        return 1000000
    else:
        return n


def amounts_cap_payday(n):
    """
    This function sets elements caps in line with product terms i.e NGN 15,000 - 250,000:
    * merchants qualifying for less than 15K get a 0 loan limit
    * merchants qualifying for more than 250k get the limits back-tracked to 250k
    """
    if n < 15000:
        return 0
    elif n > 250000:
        return 250000
    else:
        return n


def get_client(config_path, client_bvn:str, govt_companies: pd.DataFrame,multinational_companies: pd.DataFrame) -> pd.DataFrame:
    # client = remita_hook.get_pandas_df(
    #     sql="""
            # select Bvn, last_salary_within_9_months, last_salary_within_6_months from remita_staging.Customers c 
            # left join (
            #     select BvnNo, 
            #         case when PaymentDate > DATE_ADD(NOW(),INTERVAL -9 MONTH) then true else false end as last_salary_within_9_months,
            #         case when PaymentDate > DATE_ADD(NOW(),INTERVAL -6 MONTH) then true else false end as last_salary_within_6_months
            #     from (
            #         select *, rank() over (partition by BvnNo order by PaymentDate desc) rnk
            #         from remita_staging.SalaryHistory sh
            #     ) rnked where rnk = 1  
            # ) slries on c.Bvn = slries.BvnNo
            # where c.Bvn = %(bvn)s limit 1
    #     """, parameters={'bvn': client_bvn}
    # )

    config = read_params(config_path)
    project_dir = config['project_dir']
    dwh_credentials = config["db_credentials"]
    prefix = "REMITA"

    logging.warning('Pull clients ...')
    sql_client = f"""
        select Bvn, last_salary_within_9_months, last_salary_within_6_months, is_govt_employee,IFNULL(is_mnc_employee, FALSE) AS is_mnc_employee from remita_staging.Customers c 
        left join (
            select BvnNo, 
                case when PaymentDate > DATE_ADD(NOW(),INTERVAL -9 MONTH) then true else false end as last_salary_within_9_months,
                case when PaymentDate > DATE_ADD(NOW(),INTERVAL -6 MONTH) then true else false end as last_salary_within_6_months,
                CASE WHEN CompanyName IN {tuple(govt_companies)} then true else false END AS is_govt_employee, 
                CASE WHEN CompanyName IN {tuple(multinational_companies)} THEN true  ELSE false END AS is_mnc_employee
            from (
                select *, rank() over (partition by BvnNo order by PaymentDate desc) rnk
                from remita_staging.SalaryHistory sh
            ) rnked where rnk = 1  
        ) slries on c.Bvn = slries.BvnNo
        where c.Bvn = %(bvn)s limit 1
        """
    client = query_dwh(sql_client, dwh_credentials, prefix, project_dir, {'bvn': client_bvn})

    client['last_salary_within_9_months'] = client['last_salary_within_9_months'].apply(lambda x: bool(x))
    client['is_mnc_employee'] = client['is_mnc_employee'].apply(lambda x: bool(x))

    return client


def create_salaries(df, new_bvns, salaries_bvns, default_date):
    lst = []

    default_date = default_date if not pd.isna(default_date) else pd.to_datetime('today')

    for x in new_bvns:
        if x in salaries_bvns:
            lst.append(df[df['BvnNo'] == x])
        else:
            df_cols = ['Id', 'PhoneNo', 'BvnNo', 'CreatedDate', 'PaymentDate', 'Amount',
                       'AccountNumber', 'BankCode', 'CompanyName', 'is_govt_employee','is_mnc_employee']
            df.loc[len(df), df_cols] = [1, '2341111111', x, default_date, default_date, 0, '67676767', '000', 'dummy_data', 0,False]
            lst.append(df.tail(1))

    return pd.concat(lst)


def add_scoring_refresh_date(df):
    """
    function to add date when scoring refresh was done
    Inputs:
        Model refresh date
    Outputs:
        new column with scoring refresh date
    """

    scoring_refresh_date = (pd.to_datetime('today').strftime("%Y-%m-%d"))
    scoring_refresh_date = pd.Timestamp(scoring_refresh_date)

    return scoring_refresh_date


def add_model_version(df):
    """
    function to add date when scoring refresh was done
    Inputs:
        Model refresh date
    Outputs:
        new column with scoring refresh date
    """
    model_version = f"2023-001[2023-01-23, {pd.to_datetime('today').date()}]"

    return model_version


def government_employees_salaries_check(salaries, max_month, default_date):
    last_6_months = salaries[salaries['offset_payment_dates'] >= max_month + relativedelta(
        months=-5)]
    
    monthly_salary_summaries_last_6_months = last_6_months.groupby(['BvnNo', 'year_month_salary_date'])['Amount'].sum().reset_index().groupby('BvnNo')['Amount'].mean().rename('average_monthly_salary_last_6_months').reset_index()

    last_9_months = salaries[salaries['offset_payment_dates'] >= max_month + relativedelta(
        months=-8)]  # & (salaries['payment_dates'] < max_month)]
    
    salaries_summaries_cols = ['count_of_payments_last_9_months',
                               'sum_of_salary_payments_last_9_months',
                               'minimum_salary_payment_last_9_months',
                               'earliest_salary_payment_date_last_9_months',
                               'latest_salary_payment_date_last_9_months',
                               'average_monthly_salary_last_9_months', 
                               'average_monthly_salary_last_6_months', 'BvnNo',
                               'latest_salary_payment_amount_last_9_months',
                               'is_qualified_on_salaries', 'is_govt_employee'] 
    
    if (last_9_months.empty) & (last_6_months.empty):
        last_9_months_salaries_summaries = pd.DataFrame()
        last_9_months_salaries_summaries[salaries_summaries_cols] = [0, 0, 0, default_date, default_date, 0, 0, salaries['BvnNo'].values[0], 0, False, 1] 
        return last_9_months_salaries_summaries
    elif (last_9_months.empty) & ~(last_6_months.empty):
        last_9_months_salaries_summaries = pd.DataFrame()
        last_9_months_salaries_summaries[salaries_summaries_cols] = [0, 0, 0, default_date, default_date, 0, 
                                                                     monthly_salary_summaries_last_6_months['average_monthly_salary_last_6_months'].values[0], 
                                                                     salaries['BvnNo'].values[0], 0, False, 1] 
        return last_9_months_salaries_summaries

    monthly_salary_summaries = last_9_months.groupby(['BvnNo', 'year_month_salary_date'])['Amount'].sum().reset_index()
    monthly_salary_summaries = monthly_salary_summaries.groupby('BvnNo')['Amount'].mean().rename(
        'average_monthly_salary_last_9_months').reset_index()
    
    monthly_salary_summaries = monthly_salary_summaries.merge(monthly_salary_summaries_last_6_months, on='BvnNo', how='outer')

    last_9_months_salaries_summaries = last_9_months.groupby(['BvnNo']).agg(
        count_of_payments_last_9_months=pd.NamedAgg('year_month_salary_date', 'nunique'),
        sum_of_salary_payments_last_9_months=pd.NamedAgg('Amount', 'sum'),
        minimum_salary_payment_last_9_months=pd.NamedAgg('Amount', 'min'),
        # median_salary_payment_last_6_months = pd.NamedAgg('Amount', 'median'),
        # mode_salary_payment_last_6_months = pd.NamedAgg('Amount', pd.Series.mode),
        earliest_salary_payment_date_last_9_months=pd.NamedAgg('payment_dates', 'min'),
        latest_salary_payment_date_last_9_months=pd.NamedAgg('payment_dates', 'max')
        ).reset_index()

    latest_payment_dates = last_9_months_salaries_summaries[['BvnNo', 'latest_salary_payment_date_last_9_months']].copy()

    latest_payment_dates.loc[:, 'year_month_latest_salary_date'] = [x.strftime('%Y-%m') for x in latest_payment_dates[
        'latest_salary_payment_date_last_9_months']]

    last_9_months = last_9_months.merge(latest_payment_dates, on = 'BvnNo')

    latest_salary_payment_amount_last_9_months = last_9_months[last_9_months['year_month_latest_salary_date'] == last_9_months['year_month_salary_date']].groupby(
        'BvnNo')['Amount'].sum().rename('latest_salary_payment_amount_last_9_months').reset_index()

    last_9_months_salaries_summaries = pd.merge(last_9_months_salaries_summaries, monthly_salary_summaries, on='BvnNo')

    last_9_months_salaries_summaries = pd.merge(last_9_months_salaries_summaries,
                                                latest_salary_payment_amount_last_9_months, on='BvnNo')

    last_9_months_salaries_summaries['is_qualified_on_salaries'] = last_9_months_salaries_summaries.apply(
        lambda x: calculate_last_9_months_salaries(x), axis=1)
    
    last_9_months_salaries_summaries['is_govt_employee'] = 1
   

    return last_9_months_salaries_summaries


def calculate_last_6_months_salaries(df):
    count = df['count_of_payments_last_6_months']
    # company_name = df['CompanyName']
    average_salary = df['average_monthly_salary_last_6_months']
    sum_salary = df['sum_of_salary_payments_last_6_months']

    if count >= 6 :
        return True
    else:
        total_salary = (average_salary * 6)
        if sum_salary >= total_salary: ## if received >= simulation --> True
            return True
        else:
            return False


def non_government_employees_salaries_check(salaries, max_month, default_date):
    last_6_months = salaries[salaries['offset_payment_dates'] >= max_month + relativedelta(
        months=-5)]  # & (salaries['payment_dates'] < max_month)]
   
    salaries_summaries_cols = ['count_of_payments_last_6_months',
                                   'sum_of_salary_payments_last_6_months',
                                   'minimum_salary_payment_last_6_months',
                                   'earliest_salary_payment_date_last_6_months',
                                   'latest_salary_payment_date_last_6_months',
                                   'average_monthly_salary_last_6_months', 'BvnNo',
                                   'latest_salary_payment_amount_last_6_months',
                                   'is_qualified_on_salaries', 'is_govt_employee']  
    
   

    if last_6_months.empty:
        last_6_months_salaries_summaries = pd.DataFrame()
        last_6_months_salaries_summaries[salaries_summaries_cols] = [0, 0, 0, default_date, default_date, 0, salaries['BvnNo'].values[0], 0, False, 0] 
        
        return last_6_months_salaries_summaries
    
    monthly_salary_summaries = last_6_months.groupby(['BvnNo', 'year_month_salary_date'])['Amount'].sum().reset_index()

    monthly_salary_summaries = monthly_salary_summaries.groupby('BvnNo')['Amount'].mean().rename(
        'average_monthly_salary_last_6_months').reset_index()

    last_6_months_salaries_summaries = last_6_months.groupby(['BvnNo']).agg(
        count_of_payments_last_6_months=pd.NamedAgg('year_month_salary_date', 'nunique'),
        sum_of_salary_payments_last_6_months=pd.NamedAgg('Amount', 'sum'),
        minimum_salary_payment_last_6_months=pd.NamedAgg('Amount', 'min'),
        # median_salary_payment_last_6_months = pd.NamedAgg('Amount', 'median'),
        # mode_salary_payment_last_6_months = pd.NamedAgg('Amount', pd.Series.mode),
        earliest_salary_payment_date_last_6_months=pd.NamedAgg('payment_dates', 'min'),
        latest_salary_payment_date_last_6_months=pd.NamedAgg('payment_dates', 'max'),
        ).reset_index()

    latest_payment_dates = last_6_months_salaries_summaries[['BvnNo', 'latest_salary_payment_date_last_6_months']].copy()

    latest_payment_dates.loc[:, 'year_month_latest_salary_date'] = [x.strftime('%Y-%m') for x in latest_payment_dates[
        'latest_salary_payment_date_last_6_months']]

    last_6_months = last_6_months.merge(latest_payment_dates, on='BvnNo')
    latest_salary_payment_amount_last_6_months = \
    last_6_months[last_6_months['year_month_latest_salary_date'] == last_6_months['year_month_salary_date']].groupby(
        'BvnNo')['Amount'].sum().rename('latest_salary_payment_amount_last_6_months').reset_index()

    last_6_months_salaries_summaries = pd.merge(last_6_months_salaries_summaries, monthly_salary_summaries, on='BvnNo')

    last_6_months_salaries_summaries = pd.merge(last_6_months_salaries_summaries,
                                                latest_salary_payment_amount_last_6_months, on='BvnNo')

    last_6_months_salaries_summaries['is_qualified_on_salaries'] = last_6_months_salaries_summaries.apply(
        lambda x: calculate_last_6_months_salaries(x), axis=1)
    
    last_6_months_salaries_summaries['is_govt_employee'] = 0

    return last_6_months_salaries_summaries



def calculate_last_3_months_30_day_salaries(df):

    count = df['count_of_payments_last_3_months']

    if count >= 2 :
        return True
    else:
        return False
    
    


def government_employees_salaries_check_30_day_product(salaries, max_month):
    
    
    
    
#     max_date = salaries['PaymentDate'].max()
    
#     max_month = pd.to_datetime(max_date.strftime('%Y-%m'))
    
    
    salaries.loc[:, 'payment_dates'] = [x.strftime('%Y-%m-%d') for x in salaries['PaymentDate']]
    salaries['payment_dates'] = pd.to_datetime(salaries['payment_dates'])
    salaries.loc[:, 'year_month_salary_date'] = [x.strftime('%Y-%m') for x in salaries['PaymentDate']]
    salaries['offset_payment_dates'] = pd.to_datetime(salaries['payment_dates'], format="%Y%m") + MonthEnd(0)
    
    
    
    
    
    last_3_months = salaries[salaries['offset_payment_dates'] >= max_month + relativedelta(
        months=-2)]
    monthly_salary_summaries = last_3_months.groupby(['BvnNo', 'year_month_salary_date'])['Amount'].sum().reset_index()
    
    monthly_salary_summaries = monthly_salary_summaries.groupby('BvnNo')['Amount'].mean().rename(
        'average_monthly_salary_last_3_months').reset_index()
    
    last_3_months_salaries_summaries = last_3_months.groupby(['BvnNo']).agg(
        count_of_payments_last_3_months=pd.NamedAgg('year_month_salary_date', 'nunique'),
        sum_of_salary_payments_last_3_months=pd.NamedAgg('Amount', 'sum'),
        minimum_salary_payment_last_3_months=pd.NamedAgg('Amount', 'min'),
        # median_salary_payment_last_6_months = pd.NamedAgg('Amount', 'median'),
        # mode_salary_payment_last_6_months = pd.NamedAgg('Amount', pd.Series.mode),
        earliest_salary_payment_date_last_3_months=pd.NamedAgg('payment_dates', 'min'),
        latest_salary_payment_date_last_3_months=pd.NamedAgg('payment_dates', 'max')
        ).reset_index()
    
    latest_payment_dates = last_3_months_salaries_summaries[['BvnNo', 'latest_salary_payment_date_last_3_months']]
    
    latest_payment_dates.loc[:, 'year_month_latest_salary_date'] = [x.strftime('%Y-%m') for x in latest_payment_dates[
        'latest_salary_payment_date_last_3_months']]
    
    last_3_months = last_3_months.merge(latest_payment_dates, on = 'BvnNo')
    
    latest_salary_payment_amount_last_3_months = last_3_months[last_3_months['year_month_latest_salary_date'] == last_3_months['year_month_salary_date']].groupby(
        'BvnNo')['Amount'].sum().rename('latest_salary_payment_amount_last_3_months').reset_index()
    
    last_3_months_salaries_summaries = pd.merge(last_3_months_salaries_summaries, monthly_salary_summaries, on='BvnNo')
    
    last_3_months_salaries_summaries = pd.merge(last_3_months_salaries_summaries,
                                                latest_salary_payment_amount_last_3_months, on='BvnNo')
    
    
    
    is_multinational_employee = salaries.loc[:, ['BvnNo', 'is_mnc_employee']]
    
    last_3_months_salaries_summaries = pd.merge(last_3_months_salaries_summaries,
                                                 is_multinational_employee, on='BvnNo')
    
    
    
    
    last_3_months_salaries_summaries['is_qualified_on_salaries'] = last_3_months_salaries_summaries.apply(
    lambda row: calculate_last_3_months_30_day_salaries(row), axis=1)


    
    return last_3_months_salaries_summaries









def calculate_additional_summaries(salaries_summaries: pd.DataFrame, loans: pd.DataFrame, time_period: int,salaries_summaries_30_day:pd.DataFrame) -> pd.DataFrame:
    monthly_expected_repayments = loans.groupby('BvnNo')['RepaymentAmount'].sum().rename(
        'total_debt_value').reset_index()
    salaries_summaries_30_day = salaries_summaries_30_day
    salaries_summaries_30_day.rename(columns={'is_qualified_on_salaries': 'pay_day_is_qualified_on_salaries'}, inplace=True)

    
    summaries = pd.merge(salaries_summaries, monthly_expected_repayments, on='BvnNo', how='left')
    
    summaries = pd.merge(summaries, salaries_summaries_30_day, on='BvnNo', how='left')

    summaries['total_debt_value'].fillna(0, inplace=True)

    # summaries['DTI'] = round(
    #     summaries['total_debt_value'] / summaries[f'average_monthly_salary_last_{time_period}_months'], 1)
    
    try:
        summaries['DTI'] = round(summaries['total_debt_value'] / summaries[f'average_monthly_salary_last_{time_period}_months'], 1)  
    except ZeroDivisionError:
        summaries['DTI'] = 0

    
#     summaries['DTI'] = np.where(summaries[f'average_monthly_salary_last_{time_period}_months'] == 0, 0, summaries['total_debt_value'] / summaries[f'average_monthly_salary_last_{time_period}_months'])
    summaries['DTI'].fillna(1, inplace=True)
    
  


    active_loans = loans[loans['OutstandingAmount'] > 0]
    all_running_loans = active_loans.groupby('BvnNo')['disbursement_dates'].nunique().rename(
        'count_of_all_running_loans').reset_index()

    summaries = pd.merge(summaries, all_running_loans, on='BvnNo', how='outer')
    summaries['count_of_all_running_loans'].fillna(0, inplace=True)

    summaries['latest_sal_ge_min_payment'] = summaries[f'latest_salary_payment_amount_last_{time_period}_months'] >= \
                                             summaries[
                                                 f'minimum_salary_payment_last_{time_period}_months']
    
    summaries['latest_sal_ge_min_payment'].fillna(1, inplace=True)
    
    
  
    summaries['latest_sal_ge_min_payment_pay_day'] = np.where(
        (summaries['latest_salary_payment_amount_last_3_months'].fillna(0) >= summaries['minimum_salary_payment_last_3_months'].fillna(0)) |
        (summaries['latest_salary_payment_amount_last_3_months'].fillna(0) >= summaries['average_monthly_salary_last_3_months'].fillna(0)),
        True,
        False
)


    limit_factor_6_months = 1
    limit_factor_3_months = 0.5
    limit_factor_1_month = 0.2

    summaries['minimum_limit_6_months'] = limit_factor_6_months * summaries[
        f'average_monthly_salary_last_{time_period}_months'] * (1 - summaries['DTI'])
    summaries['minimum_limit_3_months'] = limit_factor_3_months * summaries[
        f'average_monthly_salary_last_{time_period}_months'] * (1 - summaries['DTI'])
    
    
    # if time_period == 9 or summaries['is_mnc_employee'] == True:
    
    if time_period == 9 or summaries['is_mnc_employee'].any():
        summaries['minimum_limit_1_month'] = limit_factor_1_month * summaries[
            'average_monthly_salary_last_3_months'] * (1 - summaries['DTI'])
        
        
        
        
           
    else:
        summaries['minimum_limit_1_month'] = 0
        
   

    return summaries


def score_client(config_path, client_bvn: str, govt_companies: pd.DataFrame,multinational_companies: pd.DataFrame) -> pd.DataFrame:
    # max_dataset_salary_payment_date = remita_hook.get_pandas_df(
    #     sql="""select max(PaymentDate) as "max_payment_date" from remita_staging.SalaryHistory"""
    # ).iloc[0]['max_payment_date']

    # salaries = remita_hook.get_pandas_df(
    #     sql=f"""
    #     WITH rnked AS (
    #         SELECT Id, PhoneNo, BvnNo, CreatedDate, PaymentDate, Amount,
    #             AccountNumber, BankCode, CompanyName, 
    #             CASE WHEN CompanyName IN {tuple(govt_companies)} then true else false END AS is_govt_employee,
    #             RANK() OVER (PARTITION BY BvnNo, PaymentDate, Amount, AccountNumber, BankCode ORDER BY CreatedDate DESC) rnk
    #         FROM remita_staging.SalaryHistory
    #         WHERE BvnNo = '{client_bvn}' AND 
    #             CASE
    #                 WHEN CompanyName IN {tuple(govt_companies)}
    #                     THEN PaymentDate >= DATE_FORMAT(DATE_SUB('{max_dataset_salary_payment_date}', INTERVAL 9 MONTH), '%Y-%m-01')
    #                 ELSE PaymentDate >= DATE_FORMAT(DATE_SUB('{max_dataset_salary_payment_date}', INTERVAL 6 MONTH), '%Y-%m-01')
    #             END
    #     )
    #     SELECT
    #         Id, PhoneNo, BvnNo, CreatedDate, PaymentDate, Amount,
    #         AccountNumber, BankCode, CompanyName, is_govt_employee
    #     FROM rnked
    #     """
    # )

    # loans = remita_hook.get_pandas_df(
    #     sql="""
    #     with rnked as (
    #         select *, rank() over (partition by PhoneNo, BvnNo, LoanProvider, LoanDisbursementDate, Status, LoanAmount, OutstandingAmount, RepaymentAmount, RepaymentFreq order by CreatedDate desc) rnk
    #         from remita_staging.LoanHistory where BvnNo = %(bvn)s
    #     ) select * from rnked where rnk = 1
    #     """,
    #     parameters={'bvn': client_bvn}
    # )

    config = read_params(config_path)
    project_dir = config['project_dir']
    dwh_credentials = config["db_credentials"]
    prefix = "REMITA"


    logging.warning('Pull max salary payment date ...')
    sql_max_dataset_salary_payment_date = f"""
        select max(PaymentDate) as "max_payment_date" from remita_staging.SalaryHistory
        """
    max_dataset_salary_payment_date = query_dwh(sql_max_dataset_salary_payment_date, dwh_credentials, prefix, project_dir)
    max_dataset_salary_payment_date = max_dataset_salary_payment_date.iloc[0]['max_payment_date']


    logging.warning('Pull salaries ...')
    sql_salaries = f"""
        WITH rnked AS (
                    SELECT Id, PhoneNo, BvnNo, CreatedDate, PaymentDate, Amount,
                        AccountNumber, BankCode, CompanyName, 
                        CASE WHEN CompanyName IN {tuple(govt_companies)} then true else false END AS is_govt_employee,
                        CASE  WHEN CompanyName IN {tuple(multinational_companies)} THEN true ELSE false END AS is_mnc_employee,
                        RANK() OVER (PARTITION BY BvnNo, PaymentDate, Amount, AccountNumber, BankCode ORDER BY CreatedDate DESC) rnk
                    FROM remita_staging.SalaryHistory
                    WHERE BvnNo = '{client_bvn}' AND 
                        CASE
                            WHEN CompanyName IN {tuple(govt_companies)}
                                THEN PaymentDate >= DATE_FORMAT(DATE_SUB('{max_dataset_salary_payment_date}', INTERVAL 9 MONTH), '%%Y-%%m-01')
                            ELSE PaymentDate >= DATE_FORMAT(DATE_SUB('{max_dataset_salary_payment_date}', INTERVAL 6 MONTH), '%%Y-%%m-01')
                        END
                )
                SELECT
                    Id, PhoneNo, BvnNo, CreatedDate, PaymentDate, Amount,
                    AccountNumber, BankCode, CompanyName, is_govt_employee, is_mnc_employee
                FROM rnked
        """
    salaries = query_dwh(sql_salaries, dwh_credentials, prefix, project_dir)
    


    logging.warning('Pull loans ...')
    sql_loans = f"""
        with rnked as (
            select *, rank() over (partition by PhoneNo, BvnNo, LoanProvider, LoanDisbursementDate, Status, LoanAmount, OutstandingAmount, RepaymentAmount, RepaymentFreq order by CreatedDate desc) rnk
            from remita_staging.LoanHistory where BvnNo = %(bvn)s
        ) select * from rnked where rnk = 1
        """
    loans = query_dwh(sql_loans, dwh_credentials, prefix, project_dir, {'bvn': client_bvn})


    loans.loc[:, 'disbursement_dates'] = [x.strftime('%Y-%m-%d') for x in loans['LoanDisbursementDate']]
    loans['disbursement_dates'] = pd.to_datetime(loans['disbursement_dates'])
    loans.loc[:, 'year_month_disbursement_dates'] = [x.strftime('%Y-%m') for x in loans['LoanDisbursementDate']]

    salaries_bvns = [x for x in salaries['BvnNo']]
    default_date = salaries['PaymentDate'].min()
    salaries = create_salaries(df=salaries, new_bvns=[client_bvn], salaries_bvns=salaries_bvns,
                               default_date=default_date)
    max_date = salaries['PaymentDate'].max()
    max_month = pd.to_datetime(max_date.strftime('%Y-%m'))
    

    current_date = max_dataset_salary_payment_date
    current_month = pd.to_datetime(current_date.strftime('%Y-%m'))

    salaries.loc[:, 'payment_dates'] = [x.strftime('%Y-%m-%d') for x in salaries['PaymentDate']]
    salaries['payment_dates'] = pd.to_datetime(salaries['payment_dates'])
    salaries.loc[:, 'year_month_salary_date'] = [x.strftime('%Y-%m') for x in salaries['PaymentDate']]

    salaries['offset_payment_dates'] = pd.to_datetime(salaries['payment_dates'], format="%Y%m") + MonthEnd(0)
    
    # salaries['offset_payment_dates'] = pd.to_datetime(salaries['payment_dates'], format="%Y%m") + pd.DateOffset(months=0)
    salaries_summaries_30_day = government_employees_salaries_check_30_day_product(salaries=salaries, max_month=max_month)
    if all(salaries['is_govt_employee'].tolist()):
        salaries_summaries = government_employees_salaries_check(salaries=salaries, max_month=current_month, default_date=default_date)
        time_period = 9
    else:
        salaries_summaries = non_government_employees_salaries_check(salaries=salaries, max_month=current_month, default_date=default_date)
        time_period = 6

    summaries = calculate_additional_summaries(
        salaries_summaries=salaries_summaries,
        loans=loans,
        time_period=time_period,salaries_summaries_30_day=salaries_summaries_30_day
    )
    

    summaries_bvns = [x for x in summaries['BvnNo']]
    no_summaries = [x for x in [client_bvn] if x not in summaries_bvns]

    summaries_cols = [f'BvnNo', f'count_of_payments_last_{time_period}_months',
                      f'count_of_payments_last_3_months',
                      f'sum_of_salary_payments_last_{time_period}_months',
                      f'sum_of_salary_payments_last_3_months',
                      f'minimum_salary_payment_last_{time_period}_months',
                      f'minimum_salary_payment_last_3_months',
                      f'earliest_salary_payment_date_last_{time_period}_months',
                      f'earliest_salary_payment_date_last_3_months',
                      f'latest_salary_payment_date_last_{time_period}_months',
                      f'latest_salary_payment_date_last_3_months',
                      f'average_monthly_salary_last_{time_period}_months',
                      f'average_monthly_salary_last_3_months',
                      f'latest_salary_payment_amount_last_{time_period}_months',
                      f'latest_salary_payment_amount_last_3_months',
                      f'is_qualified_on_salaries',f'pay_day_is_qualified_on_salaries', f'total_debt_value', f'DTI',
                      f'count_of_all_running_loans', f'latest_sal_ge_min_payment',f'latest_sal_ge_min_payment_pay_day'
                      f'minimum_limit_6_months', f'minimum_limit_3_months',
                      f'minimum_limit_1_month', f'is_govt_employee',f'is_mnc_employee']
    for x in no_summaries:
        
        
       # this area might have issues
#          summaries.loc[len(summaries), summaries_cols] = [x, 0, 0, 0, 0, 0, 0, default_date, default_date, default_date, default_date, 0, 0, 0, 0, False, False, 0, 0, 0, False,False, 0, 0, 0,1 if time_period == 9 else 0,False]
        
           summaries.loc[len(summaries), summaries_cols] = [
    x, 0, 0, 0, 0, 0, 0, default_date, default_date, default_date, default_date, 0, 0, 0, 0, False, False, 0, 0, 0, False, False, 0, 0, 0, 1 if time_period == 9 else 0, False
]




    summaries['rounded_limit_6_months'] = summaries['minimum_limit_6_months'].apply(round_off)
    summaries['rounded_limit_3_months'] = summaries['minimum_limit_3_months'].apply(round_off)
    summaries['rounded_limit_1_month'] = summaries['minimum_limit_1_month'].apply(round_off)

    summaries['final_allocated_limit_6_months'] = summaries['rounded_limit_6_months'].apply(amounts_cap)
    summaries['final_allocated_limit_3_months'] = summaries['rounded_limit_3_months'].apply(amounts_cap)
    summaries['final_allocated_limit_1_month'] = summaries['rounded_limit_1_month'].apply(amounts_cap_payday)

    summaries["model_version"] = add_model_version(summaries)
    summaries["scoring_refresh_date"] = add_scoring_refresh_date(summaries)

    # summaries_bvns = [x for x in summaries['BvnNo']]
    # no_summaries = [x for x in [client_bvn] if x not in summaries_bvns]

    # for x in no_summaries:
    #     summaries.loc[len(summaries)] = [x, 0, default_date, 0, 0, 1, 0, default_date, 0, 0, 0, 0, False, False, 0,
    #                                      0, 0]

    summaries['is_qualified'] = False
    summaries['is_3_months_qualified'] = False
    summaries['is_6_months_qualified'] = False
    summaries['is_1_month_qualified'] = False

    summaries.loc[
        (summaries['is_qualified_on_salaries'] == True) &
        (summaries['DTI'] <= 0.5) &
        (summaries['latest_sal_ge_min_payment'] == True) &
        (summaries['count_of_all_running_loans'] <= 5) &
        ~(summaries['final_allocated_limit_6_months'] > 0) &
        (summaries['final_allocated_limit_3_months'] > 0), 'is_3_months_qualified'] = True

    summaries.loc[
        (summaries['is_qualified_on_salaries'] == True) &
        (summaries['DTI'] <= 0.5) &
        (summaries['latest_sal_ge_min_payment'] == True) &
        (summaries['count_of_all_running_loans'] <= 5) &
        (summaries['final_allocated_limit_6_months'] > 0) &
        (summaries['final_allocated_limit_3_months'] > 0), 'is_6_months_qualified'] = True
    
    if time_period == 9 or summaries['is_mnc_employee'].any():
            
        summaries.loc[
            (summaries['pay_day_is_qualified_on_salaries'] == True) &
            (summaries['DTI'] <= 0.5) &
            (summaries['latest_sal_ge_min_payment_pay_day'] == True) &
            (summaries['count_of_all_running_loans'] <= 5) &
            (summaries['final_allocated_limit_1_month'] > 0), 'is_1_month_qualified'] = True
            
            
        
    
    summaries['is_qualified'] = np.where((summaries['is_3_months_qualified'] == True) | 
                                         (summaries['is_6_months_qualified'] == True), True,  summaries['is_qualified'])
    
    return summaries


def determine_limit_tenure_to_share(summaries: pd.DataFrame) -> pd.DataFrame:
    loan_count = summaries['count_of_loans'].iloc[0]
    one_month_limit = summaries['final_allocated_limit_1_month'].iloc[0]
    three_month_limit = summaries['final_allocated_limit_3_months'].iloc[0]
    six_month_limit = summaries['final_allocated_limit_6_months'].iloc[0]

    # if summaries.iloc[0]['final_is_qualified']:
    #     if loan_count <= 2:
    #         summaries['limit_to_share'] = three_month_limit
    #         summaries['tenure'] = 3
    #     else:
    #         summaries['limit_to_share'] = six_month_limit
    #         summaries['tenure'] = 6
    # else:
    #     summaries['limit_to_share'] = 0
    #     summaries['tenure'] = -1
    
    if summaries['is_3_months_qualified'].iloc[0]:
        summaries['limit_to_share'] = three_month_limit
        summaries['tenure'] = 3
    elif summaries['is_6_months_qualified'].iloc[0]:
        if loan_count <= 2:
            summaries['limit_to_share'] = three_month_limit
            summaries['tenure'] = 3
        else:
            summaries['limit_to_share'] = six_month_limit
            summaries['tenure'] = 6
    else:
        summaries['limit_to_share'] = 0
        summaries['tenure'] = -1
    
    if summaries['is_1_month_qualified'].iloc[0]:
        summaries['payday_limit_to_share'] = one_month_limit
    else:
        summaries['payday_limit_to_share'] = 0
    
    return summaries


def get_count_of_loans(config_path, bvn) -> int:
    # count_of_loans = mifos_hook.get_pandas_df(
    #     sql="""
    #         select count(*) as count_of_loans from `mifostenant-pronto`.m_client mc 
    #         left join `mifostenant-pronto`.m_loan ml on ml.client_id = mc.id
    #         where mc.external_id = %(bvn)s and ml.product_id = 15 and ml.loan_status_id in (300,600,700)
    #         and ml.disbursedon_date is not null
    #     """,
    #     parameters={'bvn': bvn}
    # )['count_of_loans'].iloc[0]

    config = read_params(config_path)
    project_dir = config['project_dir']
    dwh_credentials = config["db_credentials"]
    prefix = "MIFOS"


    logging.warning('Pull salary payment date ...')
    sql_count_of_loans = f"""
        select count(*) as count_of_loans from `mifostenant-pronto`.m_client mc 
            left join `mifostenant-pronto`.m_loan ml on ml.client_id = mc.id
            where mc.external_id = %(bvn)s and ml.product_id = 15 and ml.loan_status_id in (300,600,700)
            and ml.disbursedon_date is not null
        """
    count_of_loans = query_dwh(sql_count_of_loans, dwh_credentials, prefix, project_dir, {'bvn': bvn})
    count_of_loans = count_of_loans['count_of_loans'].iloc[0]

    return count_of_loans


def store_generated_limits(config_path, summaries):
    config = read_params(config_path)
    project_dir = config['project_dir']
    dwh_credentials = config["db_credentials"]
    upload_data_config = config["upload_data_config"]
    prefix = upload_data_config["prefix"]

    summaries.rename(columns={
        'BvnNo': 'bvn_no'
    }, inplace=True)

    summaries = summaries.reindex()
    summaries.replace({np.NAN: None}, inplace=True)

    logging.warning('Upload scoring results ...')
    # display(summaries)

    # warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

    # warehouse_hook.insert_rows(
    #     table='remita.scoring_results_remita',
    #     target_fields=summaries.reindex().columns.tolist(),
    #     replace=False,
    #     rows=tuple(summaries.reindex().replace({np.NAN: None}).itertuples(index=False)),
    #     commit_every=100
    # )

    # response = post_to_dwh(summaries, dwh_credentials, upload_data_config, prefix, project_dir)

    # print('')
    # logging.warning(f'--------------- Store generated limits response ---------------\n : {response}')


def rules_summary_narration(df):
    salaries_check = df['is_qualified_on_salaries']
    dti = df['DTI']
    latest_sal_ge_min_payment = df['latest_sal_ge_min_payment']
    count_of_all_running_loans = df['count_of_all_running_loans']
    crb_check = df['is_crb_qualified']
    final_allocated_limit_6_months = df['final_allocated_limit_6_months']
    final_allocated_limit_3_months = df['final_allocated_limit_3_months']
    final_is_qualified = df['final_is_qualified']

    if not salaries_check:
        return 'Client has not received consistent salary payment in recent months: B005'
    elif not crb_check:
        return 'Inadequate credit bureau risk profile: B002'
    elif not latest_sal_ge_min_payment:
        return 'Client has not received consistent salary payment in recent months: B005'
    elif dti > 0.5:
        return 'Client has other running loans : AAA'
    elif count_of_all_running_loans > 5 :
        return 'Client has other running loans : AAA'
    elif (final_allocated_limit_6_months > 0 or final_allocated_limit_3_months > 0) and final_is_qualified == True:
        return 'Limits assigned per lending criteria : F001'
    else:
        return 'Limits assigned less than product thresholds: D001'
    


def get_scoring_results(config_path, bvn):
    try:
        # govt_companies = warehouse_hook.get_pandas_df(
        #     sql="""select business_name from remita.company_dimension where is_federal_govt"""
        # )['business_name'].tolist()

        print('')
        logging.warning(f'Scoring {bvn} ...')

        config = read_params(config_path)
        project_dir = config['project_dir']
        dwh_credentials = config["db_credentials"]
        scoring_response_data_path_json = config["scoring_response_data_path_json"]
        prefix = "DWH"

        logging.warning('Pull government companies ...')
        sql_govt_companies = f"""
            select business_name from remita.company_dimension where is_federal_govt
            """

        logging.warning('Pull multinational companies ...')
        sql_multinational_companies = f"""
            select business_name from remita.company_dimension where is_multinational
            """

        govt_companies = query_dwh(sql_govt_companies, dwh_credentials, prefix, project_dir)
        govt_companies = govt_companies['business_name'].tolist()
        multinational_companies = query_dwh(sql_multinational_companies, dwh_credentials, prefix, project_dir)
        multinational_companies =  multinational_companies['business_name'].tolist()

        client = get_client(config_path, client_bvn=bvn,        govt_companies=govt_companies,multinational_companies=multinational_companies)


#         if not client.empty and 'is_govt_employee' in client.columns:
#             is_govt_employee = client['is_govt_employee'].iloc[0]
#         else:
#              is_govt_employee = 0 



#         if not client.empty and 'is_mnc_employee' in client.columns:
#             is_mnc_employee = client['is_mnc_employee'].iloc[0]
#         else:
#              is_mnc_employee = 0 




        # is_govt_employee = client['is_govt_employee'].iloc[0]
        # client.drop(columns=['is_govt_employee'], inplace=True)
        # is_mnc_employee = client['is_mnc_employee'].iloc[0]
        # client.drop(columns=['is_mnc_employee'], inplace=True)



        if 'is_govt_employee' in client.columns:
            is_govt_employee = client['is_govt_employee'].iloc[0]
            client.drop(columns=['is_govt_employee'], inplace=True)
        else:
            is_govt_employee = None  

        if 'is_mnc_employee' in client.columns:
            is_mnc_employee = client['is_mnc_employee'].iloc[0]
            client.drop(columns=['is_mnc_employee'], inplace=True)
        else:
            is_mnc_employee = False



        extra = {'limit_reason': '', 'payday_limit_reason': '', 'final_is_qualified': False, 'payday_final_is_qualified': False, 'is_govt_employee': str(is_govt_employee),'is_mnc_employee':str(is_mnc_employee)}
        if not client.empty:
            if bool(client.iloc[0]['last_salary_within_9_months']) or bool(client.iloc[0]['last_salary_within_6_months']):
                summaries = score_client(config_path, client_bvn=client.iloc[0]['Bvn'],     govt_companies=govt_companies,multinational_companies=multinational_companies)
                summaries['count_of_loans'] = get_count_of_loans(config_path, client.iloc[0]['Bvn'])

                # Additional CRB Logic
                crb_data_results = get_additional_crb_logic(
                    config_path, client_bvn=bvn, scoring_summaries=summaries
                )['crb_data_results']





                if summaries.iloc[0]['is_qualified']:
                    if crb_data_results.iloc[0]['is_crb_qualified']:
                        extra['limit_reason'] += 'Success. '
                        extra['final_is_qualified'] = True
                    else:
                        extra['limit_reason'] += 'Your CRC data does not qualify you for a limit. '
                else:
                    extra['limit_reason'] += 'Client does not pass business rules. '

                if summaries.iloc[0]['is_1_month_qualified']:
                    if crb_data_results.iloc[0]['is_crb_qualified'] & crb_data_results.iloc[0]['is_pay_day_crb_qualified']:
                        extra['payday_final_is_qualified'] = True
                        if extra['is_govt_employee'] == '1' or extra['is_mnc_employee']:
                            extra['payday_limit_reason'] = extra['limit_reason']
                        else:
                            extra['payday_limit_reason'] = 'Not eligible for product'
                    else:
                        if extra['is_govt_employee'] == '1' or extra['is_mnc_employee']:
                            extra['payday_limit_reason'] = extra['limit_reason']
                        else:
                            extra['payday_limit_reason'] = 'Not eligible for product'
                else:
                    if extra['is_govt_employee'] == '1' or extra['is_mnc_employee']:
                        extra['payday_limit_reason'] = extra['limit_reason']
                    else:
                        extra['payday_limit_reason'] = 'Not eligible for product'

                final_df = summaries.astype({'BvnNo': str}).merge(
                    crb_data_results.astype({'bvn_no': str}).rename(columns={
                        'total_no_of_facilities': 'crb_total_no_of_running_facilities',
                        'total_overdue_amount': 'crb_total_overdue_amount',
                        'maximum_days_in_arrears': 'crb_maximum_days_in_arrears',
                        'total_number_of_institutions': 'crb_total_number_of_institutions',
                        'summary_total_overdue_amount': 'crb_summary_total_overdue_amount',
                    }),
                    left_on='BvnNo',
                    right_on='bvn_no',
                # ).drop(columns=['bvn_no', 'crb_individual_loan_summaries'])
                ).drop(columns=['bvn_no'])

                final_df['final_is_qualified'] = extra['final_is_qualified']
                final_df['payday_final_is_qualified'] = extra['payday_final_is_qualified']
                final_df['is_mnc_employee'] = extra['is_mnc_employee']

                final_df['rules_summary_narration'] = final_df.apply(lambda x: rules_summary_narration(x), axis=1)
                final_df[["rules_summary_narration", "limit_reason"]] = final_df["rules_summary_narration"].astype("str").str.split(":", expand=True)

                if extra['is_govt_employee'] == '1'or extra['is_mnc_employee'] :
                    final_df['payday_rules_summary_narration'] = final_df.apply(lambda x: rules_summary_narration(x), axis=1)
                    final_df[["payday_rules_summary_narration", "payday_limit_reason"]] = final_df["payday_rules_summary_narration"].astype("str").str.split(":", expand=True)
                else:
                    final_df["payday_rules_summary_narration"] = 'Not eligible for product'
                    final_df["payday_limit_reason"] = 'Not eligible for product'

                final_df = determine_limit_tenure_to_share(final_df)
                final_df = final_df.drop_duplicates()
#                 keywords = ["limit", "employee", "qualified"]

#                 selected_columns = [col for col in final_df.columns if any(keyword in col.lower() for keyword in keywords)]
#                 new_df = final_df['BvnNO',selected_columns]
#                 new_df.to_csv('new_dataset.csv', index=False)
                                # store_generated_limits(config_path, final_df)
                
                
            else:
                extra['limit_reason'] += 'Not Scored. Client has not received salary in most recent month(s)'
                if extra['is_govt_employee'] == '1' or extra['is_mnc_employee']:
                    extra['payday_limit_reason'] = extra['limit_reason']
                else:
                    extra['payday_limit_reason'] = 'Not eligible for product'
        else:
            extra['limit_reason'] += 'Not Scored. Client not found'
            if extra['is_govt_employee'] == '1' or extra['is_mnc_employee']:
                extra['payday_limit_reason'] = extra['limit_reason']
            else:
                extra['payday_limit_reason'] = 'Not eligible for product'

        with open(project_dir + scoring_response_data_path_json, 'w', encoding='utf-8') as f:
            json.dump(extra, f, ensure_ascii=False, indent=4)

        print('yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy')
        # print(final_df)
        # final_df.to_csv('Waan_final.csv', index=False)

        # with open('Waan_final.csv', 'a', newline='') as file:
        #     final_df.to_csv(file, header=False, index=False)        final_df.to_csv(file, header=False, index=False)





        print(extra)
        
        logging.warning(f'--------------- Scoring response ---------------\n {extra}')
        logging.warning(f'--------------- Scoring response ---------------\n {extra}')

        # return extra
        # return pd.crb_data_results



        # return client,final_df, crb_data_results, extra
        return final_df #extra
    except Exception as e:
        # Handle the exception and return a DataFrame with zeros
        error_message = str(e)
        if 'final_df' not in locals() and 'final_df' not in globals():
            columns = []  # Add the column names you expect in 'final_df'
            zeros = [0] * len(columns)
            return pd.DataFrame([zeros], columns=columns)
        else:
            return final_df
# def run_scoring_for_bvn_numbers(config_path, bvn_numbers):
#     for bvn in bvn_numbers:
#         final_df = get_scoring_results(config_path, bvn)
        
#         keywords = ["limit", "employee", "qualified"]

#         selected_columns = [col for col in final_df.columns if any(keyword in col.lower() for keyword in keywords)]
#         new_df = final_df[['BvnNo'] + selected_columns]
#         with open('Waan_final.csv', 'a', newline='') as file:
#             new_df.to_csv(file, header=False, index=True)
def run_scoring_for_bvn_numbers(config_path, bvn_numbers):
    for bvn in bvn_numbers:
        try:
            final_df = get_scoring_results(config_path, bvn)
            
            keywords = ["limit", "employee", "qualified"]

            selected_columns = [col for col in final_df.columns if any(keyword in col.lower() for keyword in keywords)]
            new_df = final_df[['BvnNo'] + selected_columns]
            with open('Waan_final.csv', 'a', newline='') as file:
                new_df.to_csv(file, header=False, index=False)
        except Exception as e:
            # Print or log the error message and continue to the next BVN number
            print(f"Error processing BVN {bvn}: {e}")
            continue




if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

 
    bvn_numbers = ['22273366088','22153958863','2227134530','22234561345',"22268885113","2227134531", "2220087024", "2220035547", "2225565149", "2228748576", "2227915901", "2225542109", "2228380515", "2224274740", "2225239611", "2226158742", "2227134532", "2227134533", "2227134534", "2227134535", "2227134536", "2227134537", "9292", "92921", "2224452", "22273266088", "22415467832", "1234", "22228450575", "12345678910", "22241348910", "22290238923", "22142602171", "22227979464", "22190745147", "22170014184", "22253049881", "22192189839", "22272581254", "22151967960", "22203493896", "22153417946", "22442504096", "22256973220", "22160339471", "22317896082", "22161076302", "22209333088", "22210636484", "22178794279", "22286202040", "22231999153", "22187480383", "22367362490", "22219089528", "22159706127", "22312111678", "22566509344", "22383444220", "22403268083", "22329236757", "22329724760", "22156181943", "22330767497", "22279791960", "22354326386", "22237205984", "22422834430", "22231561608", "22374136653","22313355501","22386515701"] #22273366088









    run_scoring_for_bvn_numbers(parsed_args.config, bvn_numbers)
    
    
# if __name__ == "__main__":
#     # Parameter arguments
#     args = argparse.ArgumentParser()
#     args.add_argument("--config", default="params.yaml")
#     # args.add_argument("--bvn", default="22156181943")
#     parsed_args = args.parse_args()

#     # extra = get_scoring_results(parsed_args.config, bvn=read_params(parsed_args.config)['test_config']['bvn'])
#     extra = get_scoring_results(parsed_args.config, bvn=read_params(parsed_args.config)['test_config']['bvn'])
    
    
    
    print('\n=============================================================================\n')