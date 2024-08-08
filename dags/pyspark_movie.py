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
        'pyspark_movie',
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

    def air_repartition(**kwargs):
        from repartition.re_partition import re_partition
        size, read_path, write_path = re_partition(kwargs['ds_nodash'], '/data/movie/extract')
        print(size)
        print(read_path)
        print(write_path)
    
    def data():
        print(data)


    def branch_fun(**kwargs):
        ld = kwargs['ds_nodash']
        import os
        home_dir = os.path.expanduser("~")
#        path = f'{home_dir}/tmp/test_parquet/load_dt={ld}'
        path = f'{home_dir}/data/movie/hive/load_dt={ld}'
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "join.df"

    # t1, t2 and t3 are examples of tasks created by instantiating operators
#    re_partition = PythonVirtualenvOperator(
    re_partition = PythonOperator(
        task_id='re.partition',
        python_callable=air_repartition,
        trigger_rule="all_done",
    )

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    join_df = BashOperator(
        task_id='join.df',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/kim1/airflow_pyspark/py/movie_join_df.py movie_join_df {{ds_nodash}}
        """,
        trigger_rule="all_done"
        )

    agg = BashOperator(
        task_id='agg',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/kim1/airflow_pyspark/py/movie_agg.py movie_agg {{ds_nodash}}
        """,
        trigger_rule="all_done"
    )
    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command="""
            rm -rf ~/data/movie/hive/load_dt={{ds_nodash}}
        """,
        trigger_rule="all_done"
    )

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
        )

#    task_err = BashOperator(
#        bash_command="""
#            DONE_PATH=~/data/done/{{ds_nodash}}
#            mkdir -p ${DONE_PATH}
#            touch ${DONE_PATH}/_DONE
#        """,
#    )

    
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')
    
    task_start >> re_partition >> branch_op 
    branch_op >> join_df >> agg >> task_end
    branch_op >> rm_dir >> join_df

