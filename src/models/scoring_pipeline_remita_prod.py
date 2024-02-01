import os
import sys
import logging
import datetime
import pendulum
import requests
from airflow import DAG
from urllib import parse
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from scoring_scripts.remita_nigeria import get_scoring_results

remita_hook = MySqlHook(mysql_conn_id='remita_server', schema='remita_staging')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'scoring_pipeline_remita',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,# '* * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2022, 6, 8, 12, 00, tzinfo=local_tz),
        tags=['scoring_pipeline'],
        description='score remita clients',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
        ### DAG SUMMARY
        DAG is triggered by Digital team to score an Interswitch Uganda client
        
        
        Required parameters
        ```
         {
            "callback_url": "https://callback/url/endpoint",
            "bvn": "22142602171"
         }
        ```
    """

    def pass_generated_limits_to_engineering(**context):
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        scoring_response = context['ti'].xcom_pull(task_ids='trigger_scoring', key='scoring_results')

        bvn = context['dag_run'].conf.get('bvn')
        callback_url = context['dag_run'].conf.get('callback_url', None)

        if scoring_response['final_is_qualified']:
            scoring_results = warehouse_hook.get_pandas_df(
                sql="""
                    select id, bvn_no, limit_to_share, final_is_qualified, tenure
                    from remita.scoring_results_remita_view where bvn_no = %(bvn)s
                """,
                parameters={'bvn': str(bvn)}
            )
            phone_numbers = remita_hook.get_pandas_df(
                sql="""
                    with rnked as (
                        select Bvn as BvnNo, MobileNumber as PhoneNo, rank() over (partition by Bvn order by CreatedDate desc) rnk
                        from remita_staging.Customers where Bvn = %(bvn)s
                    ) select * from rnked where rnk = 1 limit 1
                """,
                parameters={'bvn': str(bvn)}
            ).drop_duplicates(subset=['PhoneNo'])

            scoring_results = scoring_results.merge(
                phone_numbers, left_on='bvn_no', right_on='BvnNo', how='left'
            ).drop(columns=['BvnNo'])
            
            

            
            
            
           

            payload = {
                "clientId": "AsanteDs0521",
                "phoneNo": str(scoring_results.iloc[0]['PhoneNo']).replace('.0', ''),
                "bvn": str(scoring_results.iloc[0]['bvn_no']).replace('.0', ''),
                "limit": scoring_results.iloc[0]['limit_to_share'],
               # f'{n}_months_limit_to_share':scoring_results.iloc[0][f'final_allocated_limits_{n}_months']
                "isQualified": bool(scoring_results.iloc[0]['final_is_qualified']),
                "createdDate": str(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                "tenure": int(scoring_results.iloc[0]['tenure']),
                "extras": {
                    "limit_reason": scoring_response['limit_reason']
                }
            }
        else:
            payload = {
                "clientId": "AsanteDs0521",
                "phoneNo": str('-1'),
                "bvn": str(bvn),
                "limit": -1,
                "isQualified": False,
                "createdDate": str(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')),
                "tenure": -1,
                "extras": {"limit_reason": scoring_response['limit_reason']}
            }

        # Share Limits
        res = requests.post(
            url=callback_url,
            headers={'Content-Type': 'application/json'},
            json=payload,
            auth=requests.auth.HTTPBasicAuth(Variable.get('remita_api_username'), Variable.get('remita_api_password')),
        )

        logging.warning(f'\n-------------------- Payload --------------------\n {payload}\n--------------- Response ---------------\n status_code: {res.status_code}\n {res.text}')


    def trigger_scoring(**context):
        client_bvn = context['dag_run'].conf.get('bvn', None)

        if client_bvn is not None:
            context['ti'].xcom_push(key='scoring_results', value=get_scoring_results(client_bvn=client_bvn))
            return True

        return False


    t1 = ShortCircuitOperator(
        task_id='trigger_scoring',
        python_callable=trigger_scoring,
        retries=0
    )
    t2 = PythonOperator(
        task_id='pass_generated_limits_to_engineering',
        provide_context=True,
        python_callable=pass_generated_limits_to_engineering
    )

    t1 >> t2
