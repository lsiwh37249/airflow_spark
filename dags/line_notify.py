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
        'line_notify',
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
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015,1,4),
    catchup=True,
    tags=['notify', 'line'],
) as dag:
    
    bash_job = BashOperator(
        task_id='bash.job',
        bash_command="""
        REMAIN=$(RANDOM % 3)
        if [ $REMAIN -eq 0 ]; then
            echo "작업에 성공했습니다."
            echo "dag_id:{{ dag_run.dag_id }}
        else
            echo "작업에 실패했습니다."
            echo "dag_id:{{ dag_run.dag_id }}
        fi    
        """
        )

    notify_success = BashOperator(
        task_id='notify.success',
        bash_command="""
        echo "notify.success"
        echo "dag_id:{{ dag_run.dag_id }}"

        curl -X POST -H 'Authorization: Bearer aNAXXJPC988TTjF12lBX6EV7p0GfGj6B8KhyPYSLik7' -F 'message={{ dag_run.dag_id}}' https://notify-api.line.me/api/notify
        """
        )

    notify_fail = BashOperator(
        task_id='notify.fail',
        bash_command="""
        echo "nofify_fail"
        echo "dag_id:{{ dag_run.dag_id }}"

        curl -X POST -H 'Authorization: Bearer aNAXXJPC988TTjF12lBX6EV7p0GfGj6B8KhyPYSLik7' -F 'message={{ dag_run.dag_id}}' https://notify-api.line.me/api/notify
        """
        )


    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> bash_job >> notify_success >> task_end
    bash_job >> notify_fail >> task_end

