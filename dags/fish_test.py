from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pprint import pprint

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
import pandas as pd
import pickle
from sklearn.neighbors import KNeighborsClassifier
import requests
import numpy as np

with DAG(
    'fish_test',
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
    tags=['api', 'fish'],
) as dag:

    def fish_request(length, weight,neighbor):

        headers = {
            'accept': 'application/json',
        }

        params = {
            'length': length,
            'weight': weight,
            'neighbor' : neighbor
        }
        print(params)

        response = requests.get('http://localhost:8765/fish', params=params, headers=headers)
        j = response.json()
        r = j.get("prediction")
        return r
        
    def to_parquet():
        fish_data = pd.read_csv('~/data/fish_test_data_100k.csv')
        # 표준점수(standard score)
        # 표준점수 = (데이터 - 평균) / 표준편차
        mean = np.mean(fish_data.iloc[:,[0,1]], axis=0)
        std = np.std(fish_data.iloc[:,[0,1]], axis=0)
        train_scaled = (fish_data.iloc[:,[0,1]] - mean) / std

        print(train_scaled)
        train_scaled.to_parquet("~/data/fish_test_data_100k.parquet")

    def predict():
        neighbor = [1,5,15,25,49]
#        neighbor =[1]
        for n in neighbor:
            df = pd.read_parquet("~/data/fish_test_data_100k.parquet")
#            print(fish_request(-2,-2,n))
            df_1 = df.iloc[:,[0,1]]  
            df_1[f"result{n}"] = df_1.apply(lambda row: fish_request(row.iloc[0], row.iloc[1], n), axis=1)
            print(df_1[f"result{n}"])
            
            df_load = pd.read_parquet("~/data/fish_parquet/fish_predict_parquet")
            df = pd.concat([df_load, df_1[[f"result{n}"]]], axis=1)

            parquet_path = "~/data/fish_parquet/fish_predict_parquet"
            df.to_parquet(parquet_path)
    
    def agg():
        neighbor = [1,5,15,25,49]
        for n in neighbor:
            df = pd.read_parquet(f"~/data/fish_parquet/fish_predict_parquet")
            r = df.groupby(f"result{n}").count()
            print(r)


    load_csv = PythonOperator(
        task_id="load.csv",
        python_callable=to_parquet
    )

    predict_task = PythonOperator(
        task_id="predict.task", 
        python_callable=predict
    )

    agg_task = PythonOperator(
        task_id="agg.task",
        python_callable=agg
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> load_csv >> predict_task >> agg_task >> task_end
