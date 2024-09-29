#import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pprint import pprint

from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
        )

with DAG(
        'kafka_evalu',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure' : False,
        'email_on_retry' : False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
        },
    max_active_tasks=3,
    max_active_runs=1,
    description='hello world DAG',
    #schedule=timedelta(days=1),
    schedule="0 9 * * *",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023,5,30),
    catchup=True,
    tags=['notify', 'line'],
) as dag:

    def log(ds_nodash, **kwargs):
        import os
        from datetime import datetime, timedelta
        import logging
        # 로그 설정 함수 포함
        def setup_logging():
            log_dir = "/home/kim1/code/logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            ds_nodash = {{ds_nodash}}
            log_file = os.path.join(log_dir, f"log_{ds_nodash}.log")
            
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
            logging.info(f"Logging is set up. Logs will be saved in {log_file}")

        # 로그 설정 실행
        setup_logging()

        # 외부 모듈 호출
        from lsiwh_simulate.customer import start
        start(logging,ds_nodash)
        
    agg_task = PythonVirtualenvOperator(
        task_id="agg.task",
        python_callable=log,
        requirements=["git+https://github.com/lsiwh37249/lsiwh_simulate.git"],
        system_site_packages=True,
        op_kwargs={'ds_nodash' : '{{ds_nodash}}'}
    ) 

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> agg_task >> task_end
