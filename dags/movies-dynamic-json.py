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
        'movies-dynamic-json',
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
    schedule="0 5 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016,1,1),
    catchup=True,
    tags=['api', 'movie'],
) as dag:

    def temp():
        print("get_data")
        return 0

    get_data = PythonOperator(
        task_id='get.data',
        python_callable=temp,
        trigger_rule="all_done",
    )

    parsing_parquet = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
            echo "parsing_parquet"
        """,
        trigger_rule="all_done"
    )

    select_parquet = BashOperator(
        task_id='select_parquet',
        bash_command="""
        echo "select_parquet"
        """,
        trigger_rule="all_done"
    )


    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> get_data >> parsing_parquet >> select_parquet >> task_end
