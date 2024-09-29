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
        'movies_dynamic_json',
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
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015,1,4),
    catchup=True,
    tags=['api', 'movie'],
) as dag:

    def fn_get_data(ds_nodash):
        print("start" * 10)
        from movapi2data.ml import data2json
        year = ds_nodash   
        data2json(year)

    get_data = PythonVirtualenvOperator(
        task_id='get.data', 
        #requirements=["git+https://github.com/lsiwh37249/mvdata.git@0.2.3/install"],
        requirements=["git+https://github.com/lsiwh37249/movapi2data.git@0.2/movie_list"],
        python_callable=fn_get_data,
        trigger_rule="all_done",
        op_kwargs = ""
    )

    parsing_parquet = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
        echo "parsing"
        f"$SPARK_HOME/bin/spark-submit /home/kim1/code/movapi2data/src/movapi2data/parse_parquet.py {{ date.ds_nodash }}"
        """,
        trigger_rule="all_done"
    )

    select_parquet = BashOperator(
        task_id='select_parquet',
        bash_command="""
        echo "select_parquet"
        f"$SPARK_HOME/bin/spark-submit /home/kim1/code/movapi2data/src/movapi2data/my_query.py spark {{ date.ds_nodash }}"
        """,
        trigger_rule="all_done"
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> get_data >> parsing_parquet >> select_parquet >> task_end
