# interaction with postgres database in airflow

# pip3 install apache-airflow-providers-postgres
import time 
import json 
from airflow import DAG 
from airflow.operators.postgres_operator import PostgresOperator 
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = { 
    'owner': 'airflow',
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    }

""" Write a DAG which creates an employee table - each row represents a new person and contains info about their name and age
then insert 3 people (with the correct metadata)
finally execute a query which calculates the average age of all employees """


create_query = """
DROP TABLE IF EXISTS employee;
CREATE TABLE employee ( id INT NOT NULL, name VARCHAR(250), age INT);
"""

# create a logic that populates the table with some data
insert_data_query = """
INSERT INTO employee (id, name, age)
values(1, 'Jack', 20),(2, 'Katy', 30),(3, 'Mark', 25);
"""

calculating_average_age = """ DROP TABLE IF EXISTS employee_avg_age AS
SELECT id, name, avg(age) AS average_age
FROM employee;
"""

dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1))

# here you will define the tasks by calling the operator
create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

group_data = PostgresOperator(task_id = "calculating_average_age", sql = calculating_average_age, dag = dag_postgres, postgres_conn_id = "postgres_derya_local")

create_table >> insert_data >> group_data