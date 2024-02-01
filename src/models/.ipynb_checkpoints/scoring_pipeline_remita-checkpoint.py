
import sys
# import pendulum
import requests
import datetime as dt
from urllib import parse
# from datetime import timedelta

# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.operators.python import PythonOperator, ShortCircuitOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.data.remita_nigeria import *
# from utils.common import on_failure
# from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
# from scoring_scripts.remita_nigeria import get_scoring_results


# remita_hook = MySqlHook(mysql_conn_id='remita_server', schema='remita_staging')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")


# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email': [],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': dt.timedelta(minutes=1),
#     'on_failure_callback': on_failure
# }

# local_tz = pendulum.timezone("Africa/Nairobi")

# with DAG(
#         'scoring_pipeline_remita',
#         default_args=default_args,
#         catchup=False,
#         schedule_interval=None,# '* * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
#         start_date=dt.datetime(2022, 6, 8, 12, 00, tzinfo=local_tz),
#         tags=['scoring_pipeline'],
#         description='score remita clients',
#         user_defined_macros=default_args,
#         max_active_runs=1
# ) as dag:
#     # DOCS
#     dag.doc_md = """
#         ### DAG SUMMARY
#         DAG is triggered by Digital team to score an Interswitch Uganda client
        
        
#         Required parameters
#         ```
#          {
#             "callback_url": "https://callback/url/endpoint",
#             "bvn": "22142602171"
#          }
#         ```
#     """


def pass_generated_limits_to_engineering(config_path, bvn, callback_url):
    # warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

    config = read_params(config_path)
    project_dir = config['project_dir']
    db_credentials = config["db_credentials"]
    scoring_response_data_path_json = config["scoring_response_data_path_json"]
    
    # scoring_response = context['ti'].xcom_pull(task_ids='trigger_scoring', key='scoring_results')
    # with open(project_dir + scoring_response_data_path_json) as f:
    #     scoring_response = json.load(f)

    # bvn = context['dag_run'].conf.get('bvn')
    # callback_url = context['dag_run'].conf.get('callback_url', None)

    # if scoring_response['is_govt_employee'] == '1'or scoring_response['is_mnc_employee']:
    #     payday_limit_reason = scoring_response['payday_limit_reason']
    # else:
    #     payday_limit_reason = 'Not eligible for product'

    # if scoring_response['final_is_qualified'] or scoring_response['payday_final_is_qualified']:
    #     # scoring_results = warehouse_hook.get_pandas_df(
    #     #     sql="""
    #     #         select id, bvn_no, payday_limit_to_share, limit_to_share, final_is_qualified, tenure
    #     #         from remita.scoring_results_remita_view where bvn_no = %(bvn)s
    #     #     """,
    #     #     parameters={'bvn': str(bvn)}
    # # )

    prefix = "DWH"

    logging.warning(f'Pull scoring results ...')
    sql_scoring_results = f"""
        select id, bvn_no, payday_limit_to_share, limit_to_share, payday_final_is_qualified, final_is_qualified,rules_summary_narration,payday_rules_summary_narration,final_is_qualified,is_3_months_qualified,is_6_months_qualified,final_allocated_limit_3_months,final_allocated_limit_6_months, tenure
            from remita.scoring_results_remita_view where bvn_no = %(bvn)s
        """
    scoring_results = query_dwh(sql_scoring_results, db_credentials, prefix, project_dir, {'bvn': str(bvn)})

    # phone_numbers = remita_hook.get_pandas_df(
    #     sql="""
    #         with rnked as (
    #             select Bvn as BvnNo, MobileNumber as PhoneNo, rank() over (partition by Bvn order by CreatedDate desc) rnk
    #             from remita_s taging.Customers where Bvn = %(bvn)s
    #         ) select * from rnked where rnk = 1 limit 1
    #     """,
    #     parameters={'bvn': str(bvn)}
    # ).drop_duplicates(subset=['PhoneNo'])

    prefix = "REMITA"

    logging.warning(f'Pull phone numbers ...')
    sql_phone_numbers = f"""
        with rnked as (
                select Bvn as BvnNo, MobileNumber as PhoneNo, rank() over (partition by Bvn order by CreatedDate desc) rnk
                from remita_staging.Customers where Bvn = %(bvn)s
            ) select * from rnked where rnk = 1 limit 1
        """
    phone_numbers = query_dwh(sql_phone_numbers, db_credentials, prefix, project_dir, {'bvn': str(bvn)})
    phone_numbers.drop_duplicates(subset=['PhoneNo'], inplace=True)

    scoring_results = scoring_results.merge(
        phone_numbers, left_on='bvn_no', right_on='BvnNo', how='left'
    ).drop(columns=['BvnNo'])

    if (scoring_results.iloc[0]['final_is_qualified_3_months'] == True).any() or \
   (scoring_results.iloc[0]['final_is_qualified_6_months'] == True).any() or \
   (scoring_results.iloc[0]['payday_final_is_qualified'] == True).any():
   

        payroll_6_month_limit=scoring_results.iloc[0]['payroll_6_months_limit_to_share'] 
            
        payroll_3_month_limit=scoring_results.iloc[0]['payroll_3_months_limit_to_share']
        
        payroll_1_month_limit=scoring_results.iloc[0]["payday_limit_to_share"] 
        
        if not scoring_results.iloc[0]["final_is_qualified_6_months"].any()== True:
            payroll_6_month_limit = 0        

        else:
            payroll_6_month_limit = payroll_6_month_limit 
            
        if not scoring_results.iloc[0]["final_is_qualified_3_months"].any()== True:
            payroll_3_month_limit = 0
        else:
            payroll_3_month_limit = payroll_3_month_limit
            
        if not  scoring_results.iloc[0]["payday_final_is_qualified"].any()== True:
            payroll_1_month_limit=0
        else:
            payroll_1_month_limit=payroll_1_month_limit
            
            

            
            
            
    
        payload = {
            "clientId": "AsanteDs0521",
            "phoneNo": str(scoring_results.iloc[0]['PhoneNo']).replace('.0', ''),
            "bvn": str(scoring_results.iloc[0]['bvn_no']).replace('.0', ''),
            "is_6_months_qualified":bool(scoring_results.iloc[0]['final_is_qualified_6_months']),
            "is_3_months_qualified":bool(scoring_results.iloc[0]['final_is_qualified_3_months']),
            "is_1_months_qualified":bool(scoring_results.iloc[0]['payday_final_is_qualified']),
            "payroll_6_month_limit":payroll_6_month_limit,
            "payroll_3_month_limit":payroll_3_month_limit,
            "payroll_1_month_limit":payroll_1_month_limit,
            "createdDate": str(dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
            "extras": {
                "payroll_6_month_limit_reason":scoring_results.iloc[0]['rules_summary_narration'],
                "payroll_3_month_limit_reason":scoring_results.iloc[0]['rules_summary_narration'],
                'payroll_1_month_limit_reason':scoring_results.iloc[0]['payday_rules_summary_narration'],
                }
            }
    else:
        


        payload = {
                "clientId": "AsanteDs0521",
                "phoneNo": str(-1),
                "bvn": str(bvn),
                "is_6_months_qualified":False,
                "is_3_months_qualified":False,
                "is_1_months_qualified":False,
                "payroll_6_month_limit":-1,
                "payroll_3_month_limit":-1,
                "payroll_1_month_limit":-1,
                "createdDate": str(dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                "extras": {
                    "payroll_6_month_limit_reason":scoring_results.iloc[0]['rules_summary_narration'],
                    "payroll_3_month_limit_reason":scoring_results.iloc[0]['rules_summary_narration'],
                    "payroll_1_month_limit_reason":scoring_results.iloc[0]['payday_rules_summary_narration'],
                    }
                }
                    
#          if scoring_results["is_final_qualified"] != True:
#             scoring_results.iloc[0]['final_allocated_limit_6_months'] = 0
#             scoring_results.iloc[0]['final_allocated_limit_3_months'] = 0

#         else:
#             if scoring_results.iloc[0]['final_allocated_limit_6_months'] == scoring_results.iloc[0]['limit_to_share']:
#                 scoring_results.iloc[0]['final_allocated_limit_3_months'] = 0
#             elif scoring_results.iloc[0]['final_allocated_limit_3_months'] == scoring_results.iloc[0]['limit_to_share']:
#                 scoring_results.iloc[0]['final_allocated_limit_6_months'] = 0

            
            
            
    
#         payload = {
#             "clientId": "AsanteDs0521",
#             "phoneNo": str(scoring_results.iloc[0]['PhoneNo']).replace('.0', ''),
#             "bvn": str(scoring_results.iloc[0]['bvn_no']).replace('.0', ''),
#             "is_6_months_qualified":bool(scoring_results.iloc[0]['final_is_qualified']),
#             "is_3_months_qualified":bool(scoring_results.iloc[0]['final_is_qualified']),
#             "is_1_months_qualified":bool(scoring_results.iloc[0]['payday_final_is_qualified']),
#             "payroll_6_month_limit":scoring_results.iloc[0]['final_allocated_limit_6_months'],
#             "payroll_3_month_limit":scoring_results.iloc[0]['final_allocated_limit_3_months'],
#             "payroll_1_month_limit":scoring_results.iloc[0]['payday_limit_to_share'],
#             "createdDate": str(dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
#             "extras": {
#                 "payroll_6_month_limit":scoring_results.iloc[0]['rules_summary_narration'],
#                 "payroll_3_month_limit":scoring_results.iloc[0]['rules_summary_narration'],
#                 'payroll_1_month_limit':scoring_results.iloc[0]['payday_rules_summary_narration'],
#                 }
#             }
#     else:
        
#          prefix = "DWH"

#         logging.warning(f'Pull scoring results ...')
#         sql_scoring_results = f"""
#             select  bvn_no, rules_summary_narration,payday_rules_summary_narration
#                 from remita.scoring_results_remita_view where bvn_no = %(bvn)s
#             """
#         scoring_results = query_dwh(sql_scoring_results, db_credentials, prefix, project_dir, {'bvn': str(bvn)})

#          payload = {
#                 "clientId": "AsanteDs0521",
#                 "phoneNo": str(-1),
#                 "bvn": str(bvn),
#                 "is_6_months_qualified":False,
#                 "is_3_months_qualified":False,
#                 "is_1_months_qualified":False,
#                 "payroll_6_month_limit":-1,
#                 "payroll_3_month_limit":-1,
#                 "payroll_1_month_limit":-1,
#                 "createdDate": str(dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
#                 "extras": {
#                     "payroll_6_month_limit":scoring_results.iloc[0]['limit_reason'],
#                     "payroll_3_month_limit":scoring_results.iloc[0]['limit_reason'],
#                     "payroll_1_month_limit":scoring_results.iloc[0]['payday_rules_summary_narration'],
#                     }
#                 }
    
    res = share_scoring_results(config_path, bvn, callback_url, payload)
    
    # logging.warning(f'\n-------------------- Payload --------------------\n {payload}\n--------------- Response ---------------\n status_code: {res.status_code}\n {res.text}')
    print('')
    logging.warning(f'-------------------- Payload --------------------\n {payload}\n')

    # def trigger_scoring(**context):
    #     client_bvn = context['dag_run'].conf.get('bvn', None)

    #     if client_bvn is not None:
    #         context['ti'].xcom_push(key='scoring_results', value=get_scoring_results(client_bvn=client_bvn))
    #         return True

    #     return False


    # t1 = ShortCircuitOperator(
    #     task_id='trigger_scoring',
    #     python_callable=trigger_scoring,
    #     retries=0
    # )
    # t2 = PythonOperator(
    #     task_id='pass_generated_limits_to_engineering',
    #     provide_context=True,
    #     python_callable=pass_generated_limits_to_engineering
    # )

    # t1 >> t2

    # t1


if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    # args.add_argument("--bvn", default="22156181943")
    # args.add_argument("--callback_url", default="https://callback/url/endpoint")
    parsed_args = args.parse_args()

    # logging.warning(f'Sharing limits for {parsed_args.bvn} ...')
    pass_generated_limits_to_engineering(parsed_args.config, bvn=read_params(parsed_args.config)['test_config']['bvn'], callback_url=read_params(parsed_args.config)['airflow_api_config']['callback_url'])
    print('\n=============================================================================\n')