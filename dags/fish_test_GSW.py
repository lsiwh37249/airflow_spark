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
from sklearn.metrics import confusion_matrix
import requests
import numpy as np
import os
import json

with DAG(
    'fish_test_GSW',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure' : False,
        'email_on_retry' : False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
        'execution_timeout' : timedelta(hours=2),
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

    def loadcsv():
        file_path = "/home/kim1/data/fish_test_data.csv"
        save_path = '/home/kim1/data/fish_parquet/'
        fish_data=pd.read_csv(file_path)
        #val_data['Label'][val_data['Label']=='Bream']=0

        # 표준점수(standard score)
        # 표준점수 = (데이터 - 평균) / 표준편차
        mean = np.mean(fish_data.iloc[:,[0,1]], axis=0)
        std = np.std(fish_data.iloc[:,[0,1]], axis=0)
        train_scaled = (fish_data.iloc[:,[0,1]] - mean) / std

        print(train_scaled)
        if os.path.exists(save_path):
            train_scaled.to_parquet(f"{save_path}/fish_test_data.parquet")
        else:
            os.makedirs(os.path.dirname(save_path), exist_ok = False)
            train_scaled.to_parquet(f"{save_path}/fish_test_data.parquet")

    def prediction():
        load_path = "/home/kim1/data/fish_parquet/"
        save_path = "/home/kim1/data/fish_pred_parquet/"

        val_data=pd.read_parquet(load_path)
        val_data_cut = val_data
#        val_data_cut = val_data.iloc[:100]
        headers = {
            'accept': 'application/json',
        }
        neighbor=[1,5,15,25,49]
        for j in neighbor:  
            pred_result=[]
#            print(f" len(val_data_cut : {len(val_data_cut)}")
            for i in range(len(val_data_cut)):
                params = {
                'neighbor' : j,
                'length': val_data_cut['Length'][i],
                'weight': val_data_cut['Weight'][i],
                }
                response = requests.get('http://127.0.0.1:8765/fish', params=params, headers=headers)
                data=json.loads(response.text)
                col_name = f"k{j}"
                if data['prediction'] == '도미':
                    pred_result.append('Bream')
                else :
                    pred_result.append('Smelt')

            val_data_cut[f'{col_name}']=pred_result

            if os.path.exists(save_path):
                val_data_cut.to_parquet(f"{save_path}/fish_pred{j}.parquet")
            else:
                os.makedirs(os.path.dirname(save_path), exist_ok = False)
                val_data_cut.to_parquet(f"{save_path}/fish_pred{j}.parquet")

    def agg():
        load_path = "/home/kim1/data/fish_pred_parquet/fish_pred49.parquet"
        save_path = "/home/kim1/data/fish_agg_parquet/"

        val_data=pd.read_parquet(load_path)
        val_data.replace({'Bream': 0, 'Smelt': 1}, inplace=True)

        print(val_data) 
        for i in range(4,8):
            if os.path.exists(save_path):
                cm_df.to_parquet(f"{save_path}/fish_agg{i}.parquet")
            else:
                os.makedirs(os.path.dirname(save_path), exist_ok = False)
                cm_df.to_parquet(f"{save_path}/fish_agg{i}.parquet")

    load_csv = PythonOperator(
        task_id="load.csv",
        python_callable=loadcsv
    )

    predict_task = PythonOperator(
        task_id="predict.task", 
        python_callable=prediction
    )

    agg_task = PythonOperator(
        task_id="agg.task",
        python_callable=agg
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> load_csv >> predict_task >> agg_task >> task_end
