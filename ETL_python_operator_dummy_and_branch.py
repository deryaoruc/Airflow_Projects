from airflow import Dag
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import requests
import time
import pandas as pd
import json
import numpy as np
import os



def get_data(**kwargs):
    api_key = "UTE7O7WD087X1A05"
    ticker = kwargs['ticker']
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + ticker + '&apikey='+api_key
    r = requests.get(url)

    try:
        data = r.json()
        path ="C:/Users/derya/OneDrive/Masaüstü/DATA_CENTER\DATA_LAKE"
        with open(path + "stock_market_raw_data_" + ticker + "_" + str(time.time()), "w") as outfile:
            json.dump(data, outfile)
    except:
        pass


def test_data_first_options(**kwargs):
    ticker = kwargs['ticker']

    read_path = "C:/Users/derya/OneDrive/Masaüstü/DATA_CENTER\DATA_LAKE"

    latest = np.max([float(file.split("_")[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    file = open(read_path, latest_file)
    data = json.load(file)

    condiction_1 = len(data.keys() == 2)
    condiction_2 = 'Time Series (Daily)' in data.keys()
    condiction_3 = 'Meta Data' in data.keys()

    if condiction_1 and condiction_2 and condiction_3:
        pass
    else:
        raise Exception("The data integrity has been comprpmised")



def test_data(**kwargs):
    ticker = kwargs['ticker']

    read_path = "C:/Users/derya/OneDrive/Masaüstü/DATA_CENTER/DATA_LAKE"

    latest = np.max([float(file.split("_")[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    file = open(read_path, latest_file)
    data = json.load(file)

    condiction_1 = len(data.keys() == 2)
    condiction_2 = 'Time Series (Daily)' in data.keys()
    condiction_3 = 'Meta Data' in data.keys()

    if condiction_1 and condiction_2 and condiction_3:
        return "clean_marker_data"
    else:
        return "failed_task_data"



def clean_market_data(**kwargs):
    ticker = kwargs['ticker']

    read_path = "C:/Users/derya/OneDrive/Masaüstü/DATA_CENTER\DATA_LAKE"

    latest = np.max([float(file.split("_")[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    output_path = "C:/Users/derya/OneDrive/Masaüstü/DATA_CENTER\CLEAN_DATA"

    file = open(read_path, latest_file)
    data = json.load(file)

    clean_data = pd.DataFrame(data['Time Series (Daily)']).T
    clean_data['ticker'] = data['Meta Data']['2. Symbol']
    clean_data['Meta Data'] = str(data['Meta Data'])
    clean_data['timestamp'] = pd.to_datetime('now')

    clean_data.to_csv(output_path + ticker + "snapshot_daily_"+ str(pd.to_datetime('now'))+ '.csv' )


default_dag_args = {
    'start_date': datetime(2022, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

with DAG("market_data_alphavantage_dag", schedule_interval='@daily', catchup=False, default_args=default_dag_args) as python_api_dag:
    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'ticker': "IBM"})
    task_1 = BranchPythonOperator(task_id = "test_market_data", python_callable = test_data, op_kwargs = {'ticker': "IBM"})

    task_2_1 = PythonOperator(task_id = "clean_market_data", python_callable = clean_market_data, op_kwargs = {'ticker': "IBM"})
    task_2_2 = DummyOperator(task_id = "failed_task_data")

    task_3 = DummyOperator(task_id = "aggreagate_all_data_to_db")

    task_0 >> task_1
    task_1 >> task_2_1 >> task_3
    task_1 >> task_2_2 >> task_3
