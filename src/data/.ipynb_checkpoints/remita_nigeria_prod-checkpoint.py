import os
import sys
import logging
import numpy as np
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from dateutil.relativedelta import relativedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scoring_scripts.crb_data_incorporation import get_additional_crb_logic


remita_hook = MySqlHook(mysql_conn_id='remita_server', database='remita_staging')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-pronto')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

def calculate_last_9_months_salaries(df):

    count = df['count_of_payments_last_9_months']

    if count >= 6 :
        return True
    else:
        return False

def round_off(n):
    import math
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


def get_client(client_bvn:str) -> pd.DataFrame:

    client = remita_hook.get_pandas_df(
        sql="""
            select Bvn, last_salary_within_9_months, last_salary_within_6_months from remita_staging.Customers c 
            left join (
                select BvnNo, 
                    case when PaymentDate > DATE_ADD(NOW(),INTERVAL -9 MONTH) then true else false end as last_salary_within_9_months,
                    case when PaymentDate > DATE_ADD(NOW(),INTERVAL -6 MONTH) then true else false end as last_salary_within_6_months
                from (
                    select *, rank() over (partition by BvnNo order by PaymentDate desc) rnk
                    from remita_staging.SalaryHistory sh
                ) rnked where rnk = 1  
            ) slries on c.Bvn = slries.BvnNo
            where c.Bvn = %(bvn)s limit 1
        """, parameters={'bvn': client_bvn}
    )
    client['last_salary_within_9_months'] = client['last_salary_within_9_months'].apply(
        lambda x: bool(x)
    )
    return client

def create_salaries(df, new_bvns, salaries_bvns, default_date):
    lst = []

    default_date = default_date if not pd.isna(default_date) else pd.to_datetime('today')

    for x in new_bvns:
        if x in salaries_bvns:
            lst.append(df[df['BvnNo'] == x])
        else:
            df.loc[len(df)] = [1, '2341111111', x, default_date, default_date, 0, '67676767', '000', 'dummy_data']
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

def government_employees_salaries_check(salaries, max_month):
    last_9_months = salaries[salaries['offset_payment_dates'] >= max_month + relativedelta(
        months=-8)]  # & (salaries['payment_dates'] < max_month)]

    monthly_salary_summaries = last_9_months.groupby(['BvnNo', 'year_month_salary_date'])['Amount'].sum().reset_index()

    monthly_salary_summaries = monthly_salary_summaries.groupby('BvnNo')['Amount'].mean().rename(
        'average_monthly_salary_last_9_months').reset_index()

    last_9_months_salaries_summaries = last_9_months.groupby(['BvnNo']).agg(
        count_of_payments_last_9_months=pd.NamedAgg('year_month_salary_date', 'nunique'),
        sum_of_salary_payments_last_9_months=pd.NamedAgg('Amount', 'sum'),
        minimum_salary_payment_last_9_months=pd.NamedAgg('Amount', 'min'),
        # median_salary_payment_last_6_months = pd.NamedAgg('Amount', 'median'),
        # mode_salary_payment_last_6_months = pd.NamedAgg('Amount', pd.Series.mode),
        earliest_salary_payment_date_last_9_months=pd.NamedAgg('payment_dates', 'min'),
        latest_salary_payment_date_last_9_months=pd.NamedAgg('payment_dates', 'max')
        ).reset_index()

    latest_payment_dates = last_9_months_salaries_summaries[['BvnNo', 'latest_salary_payment_date_last_9_months']]

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

    return last_9_months_salaries_summaries

def calculate_last_6_months_salaries(df):

    count = df['count_of_payments_last_6_months']
    #company_name = df['CompanyName']
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

def non_government_employees_salaries_check(salaries, max_month):
    last_6_months = salaries[salaries['offset_payment_dates'] >= max_month + relativedelta(
        months=-5)]  # & (salaries['payment_dates'] < max_month)]

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

    latest_payment_dates = last_6_months_salaries_summaries[['BvnNo', 'latest_salary_payment_date_last_6_months']]

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

    return last_6_months_salaries_summaries

def calculate_additional_summaries(salaries_summaries: pd.DataFrame, loans: pd.DataFrame, time_period: int) -> pd.DataFrame:
    monthly_expected_repayments = loans.groupby('BvnNo')['RepaymentAmount'].sum().rename(
        'total_debt_value').reset_index()
    summaries = pd.merge(salaries_summaries, monthly_expected_repayments, on='BvnNo', how='left')

    summaries['total_debt_value'].fillna(0, inplace=True)

    summaries['DTI'] = round(
        summaries['total_debt_value'] / summaries[f'average_monthly_salary_last_{time_period}_months'], 1)
    summaries['DTI'].fillna(1, inplace=True)

    active_loans = loans[loans['OutstandingAmount'] > 0]
    all_running_loans = active_loans.groupby('BvnNo')['disbursement_dates'].nunique().rename(
        'count_of_all_running_loans').reset_index()

    summaries = pd.merge(summaries, all_running_loans, on='BvnNo', how='outer')
    summaries['count_of_all_running_loans'].fillna(0, inplace=True)

    summaries['latest_sal_ge_min_payment'] = summaries[f'latest_salary_payment_amount_last_{time_period}_months'] >= \
                                             summaries[
                                                 f'minimum_salary_payment_last_{time_period}_months']

    limit_factor_6_months = 1
    limit_factor_3_months = 0.5

    summaries['minimum_limit_6_months'] = limit_factor_6_months * summaries[
        f'average_monthly_salary_last_{time_period}_months'] * (1 - summaries['DTI'])
    summaries['minimum_limit_3_months'] = limit_factor_3_months * summaries[
        f'average_monthly_salary_last_{time_period}_months'] * (1 - summaries['DTI'])

    return summaries

def score_client(client_bvn: str, govt_companies: pd.DataFrame) -> pd.DataFrame:
    max_dataset_salary_payment_date = remita_hook.get_pandas_df(
        sql="""select max(PaymentDate) as "max_payment_date" from remita_staging.SalaryHistory"""
    ).iloc[0]['max_payment_date']

    salaries = remita_hook.get_pandas_df(
        sql=f"""
        WITH rnked AS (
            SELECT Id, PhoneNo, BvnNo, CreatedDate, PaymentDate, Amount,
                AccountNumber, BankCode, CompanyName, 
                CASE WHEN CompanyName IN {tuple(govt_companies)} then true else false END AS is_govt_employee,
                RANK() OVER (PARTITION BY BvnNo, PaymentDate, Amount, AccountNumber, BankCode ORDER BY CreatedDate DESC) rnk
            FROM remita_staging.SalaryHistory
            WHERE BvnNo = '{client_bvn}' AND 
                CASE
                    WHEN CompanyName IN {tuple(govt_companies)}
                        THEN PaymentDate >= DATE_FORMAT(DATE_SUB('{max_dataset_salary_payment_date}', INTERVAL 9 MONTH), '%Y-%m-01')
                    ELSE PaymentDate >= DATE_FORMAT(DATE_SUB('{max_dataset_salary_payment_date}', INTERVAL 6 MONTH), '%Y-%m-01')
                END
        )
        SELECT
            Id, PhoneNo, BvnNo, CreatedDate, PaymentDate, Amount,
            AccountNumber, BankCode, CompanyName, is_govt_employee
        FROM rnked
        """
    )

    loans = remita_hook.get_pandas_df(
        sql="""
        with rnked as (
            select *, rank() over (partition by PhoneNo, BvnNo, LoanProvider, LoanDisbursementDate, Status, LoanAmount, OutstandingAmount, RepaymentAmount, RepaymentFreq order by CreatedDate desc) rnk
            from remita_staging.LoanHistory where BvnNo = %(bvn)s
        ) select * from rnked where rnk = 1
        """,
        parameters={'bvn': client_bvn}
    )

    loans.loc[:, 'disbursement_dates'] = [x.strftime('%Y-%m-%d') for x in loans['LoanDisbursementDate']]
    loans['disbursement_dates'] = pd.to_datetime(loans['disbursement_dates'])
    loans.loc[:, 'year_month_disbursement_dates'] = [x.strftime('%Y-%m') for x in loans['LoanDisbursementDate']]


    salaries_bvns = [x for x in salaries['BvnNo']]
    default_date = salaries['PaymentDate'].min()
    salaries = create_salaries(df=salaries, new_bvns=[client_bvn], salaries_bvns=salaries_bvns,
                               default_date=default_date)

    current_date = max_dataset_salary_payment_date
    current_month = pd.to_datetime(current_date.strftime('%Y-%m'))


    salaries.loc[:, 'payment_dates'] = [x.strftime('%Y-%m-%d') for x in salaries['PaymentDate']]
    salaries['payment_dates'] = pd.to_datetime(salaries['payment_dates'])
    salaries.loc[:, 'year_month_salary_date'] = [x.strftime('%Y-%m') for x in salaries['PaymentDate']]
    salaries['offset_payment_dates'] = pd.to_datetime(salaries['payment_dates'], format="%Y%m") + MonthEnd(0)

    if all(salaries['is_govt_employee'].tolist()):
        salaries_summaries = government_employees_salaries_check(salaries=salaries, max_month=current_month)
        time_period = 9
    else:
        salaries_summaries = non_government_employees_salaries_check(salaries=salaries, max_month=current_month)
        time_period = 6

    summaries = calculate_additional_summaries(
        salaries_summaries=salaries_summaries,
        loans=loans,
        time_period=time_period
    )

    summaries['rounded_limit_6_months'] = summaries['minimum_limit_6_months'].apply(round_off)
    summaries['rounded_limit_3_months'] = summaries['minimum_limit_3_months'].apply(round_off)

    summaries['final_allocated_limit_6_months'] = summaries['rounded_limit_6_months'].apply(amounts_cap)
    summaries['final_allocated_limit_3_months'] = summaries['rounded_limit_3_months'].apply(amounts_cap)

    summaries["model_version"] = add_model_version(summaries)
    summaries["scoring_refresh_date"] = add_scoring_refresh_date(summaries)


    summaries_bvns = [x for x in summaries['BvnNo']]
    no_summaries = [x for x in [client_bvn] if x not in summaries_bvns]

    for x in no_summaries:
        summaries.loc[len(summaries)] = [x, 0, default_date, 0, 0, 1, 0, default_date, 0, 0, 0, 0, False, False, 0,
                                         0, 0]

    summaries['is_qualified'] = False
    summaries.loc[
        (summaries['is_qualified_on_salaries'] == True) &
        (summaries['DTI'] <= 0.5) &
        (summaries['latest_sal_ge_min_payment'] == True) &
        (summaries['count_of_all_running_loans'] <= 5) &
        (summaries['final_allocated_limit_6_months'] > 0) &
        (summaries['final_allocated_limit_3_months'] > 0), 'is_qualified'] = True

    return summaries

def determine_limit_tenure_to_share(summaries: pd.DataFrame) -> None:
    loan_count = summaries['count_of_loans'].iloc[0]
    three_month_limit = summaries['final_allocated_limit_3_months'].iloc[0]
    six_month_limit = summaries['final_allocated_limit_6_months'].iloc[0]

    if summaries.iloc[0]['final_is_qualified']:
        if loan_count <= 2:
            summaries['limit_to_share'] = three_month_limit
            summaries['tenure'] = 3
        else:
            summaries['limit_to_share'] = six_month_limit
            summaries['tenure'] = 6
    else:
        summaries['limit_to_share'] = 0
        summaries['tenure'] = -1

def get_count_of_loans(bvn) -> int:
    count_of_loans = mifos_hook.get_pandas_df(
        sql="""
            select count(*) as count_of_loans from `mifostenant-pronto`.m_client mc 
            left join `mifostenant-pronto`.m_loan ml on ml.client_id = mc.id
            where mc.external_id = %(bvn)s and ml.product_id = 15 and ml.loan_status_id in (300,600,700)
            and ml.disbursedon_date is not null
        """,
        parameters={'bvn': bvn}
    )['count_of_loans'].iloc[0]

    return count_of_loans


def store_generated_limits(summaries):
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
    summaries.rename(columns={
        'BvnNo': 'bvn_no'
    }, inplace=True)

    warehouse_hook.insert_rows(
        table='remita.scoring_results_remita',
        target_fields=summaries.reindex().columns.tolist(),
        replace=False,
        rows=tuple(summaries.reindex().replace({np.NAN: None}).itertuples(index=False)),
        commit_every=100
    )

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

def get_scoring_results(client_bvn):
    govt_companies = warehouse_hook.get_pandas_df(
        sql="""select business_name from remita.company_dimension where is_federal_govt"""
    )['business_name'].tolist()

    client = get_client(client_bvn=client_bvn)

    extra = {'limit_reason': '', 'final_is_qualified': False}
    if not client.empty:
        if bool(client.iloc[0]['last_salary_within_9_months']) or bool(client.iloc[0]['last_salary_within_6_months']):
            summaries = score_client(client_bvn=client.iloc[0]['Bvn'], govt_companies=govt_companies)
            summaries['count_of_loans'] = get_count_of_loans(client.iloc[0]['Bvn'])

            # Additional CRB Logic
            crb_data_results = get_additional_crb_logic(
                client_bvn=client_bvn, scoring_summaries=summaries
            )['crb_data_results']

            if summaries.iloc[0]['is_qualified']:
                if crb_data_results.iloc[0]['is_crb_qualified']:
                    extra['limit_reason'] += 'Success. '
                    extra['final_is_qualified'] = True
                else:
                    extra['limit_reason'] += 'Your CRC data does not qualify you for a limit. '
            else:
                extra['limit_reason'] += 'Client does not pass business rules. '

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
            ).drop(columns=['bvn_no', 'crb_individual_loan_summaries'])
            final_df['final_is_qualified'] = extra['final_is_qualified']

            final_df['rules_summary_narration'] = final_df.apply(lambda x: rules_summary_narration(x), axis=1)
            final_df[["rules_summary_narration", "limit_reason"]] = final_df["rules_summary_narration"].astype("str").str.split(":", expand=True)

            determine_limit_tenure_to_share(final_df)
            store_generated_limits(final_df)
        else:
            extra['limit_reason'] += 'Not Scored. Client has not received salary in most recent month(s)'
    else:
        extra['limit_reason'] += 'Not Scored. Client not found'
    return extra
