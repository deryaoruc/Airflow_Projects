import time 
import json
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


create_query = """
DROP TABLE IF EXISTS public.stock_market_daily;
CREATE TABLE public.stock_market_daily ( id INT NOT NULL, ticker VARCHAR(250), price_open FLOAT);
"""

insert_data_query = """
INSERT INTO public.stock_market_daily (id, ticker, price_open)
values(1, 'IBM', 200),(2, 'TESLA', 300),(3, 'IBM', 100),
(4, 'TESLA', 200),(5, 'IBM', 300),(6, 'TESLA', 200);
"""

create_grouped_table = """
DROP TABLE IF EXİSTS ticker_aggregated_data AS
SELECT ticker, avg(price_open)
FROM stock_market_daily
GROUP BY 1;
"""

dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date  = days_ago(1))

create_table = PostgresOperator(task_id= "creation_of_table", 
    sql = create_query, 
    dag = dag_postgres, 
    postgres_conn_id = "postgres_derya_local")

insert_data = PostgresOperator(task_id= "insertion_of_data", 
    sql = insert_data_query, 
    dag = dag_postgres, 
    postgres_conn_id = "postgres_derya_local")

group_data = PostgresOperator(task_id= "creating_grouped_table", 
    sql = create_grouped_table, 
    dag = dag_postgres, 
    postgres_conn_id = "postgres_derya_local")

group_data_2 = PostgresOperator(task_id= "creating_grouped_table_2", 
    sql = create_grouped_table_2, 
    dag = dag_postgres, 
    postgres_conn_id = "postgres_derya_local")

create_table >> insert_data >> group_data
insert_data >> group_data_2