from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
        )
#import pandas as pd
#import pyarrow as pa
from json import dump, loads
#import os

with DAG(
        'sche_alarm',
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
    schedule="15 9 * * *",
    start_date=datetime(2024,8,26),
    end_date=datetime(2024,8,30),
    catchup=True,
    tags=['alarm', 'schedule'],
) as dag:

    def pro_send():
        from kafka import KafkaProducer
        import time
        from json import dumps

        p = KafkaProducer(
            #bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            bootstrap_servers=['localhost:9092'],
            value_serializer = lambda x:dumps(x,default=str).encode('utf-8')
        )
        username = "칸반 알리미"
        msg = "현재는 9시 15분 : 칸반 시간입니다"
        
        data = {'username' : username, 'message' : msg, 'time' : time.time()}
        print("++++++++++++++++++++++++") 
        p.send('haha', value = data)
        print("++++++++++++++++++++++++") 
        p.flush()


    def gen_emp(id, rule='all_success'):
        op=EmptyOperator(task_id=id, trigger_rule=rule)
        return op

    start=gen_emp('start')
    end=gen_emp('end', 'all_done')

    def alarm():
        print("알람")

    task_alarm=PythonOperator(
        task_id='save.parquet',
        python_callable=pro_send
            )
    
    start >> task_alarm >> end
