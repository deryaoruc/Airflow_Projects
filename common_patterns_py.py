from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta


default_dag_args = {
    'start_date': datetime(2022, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

def choice_function():
    x > 10
    if x > 11:
        return "task_4"
    else:
        return "task_5"


new_dag = DAG(dag_id="common_airflow_patterns_class", default_args=default_dag_args, schedule_interval=None)

# sequence of tasks
""" with new_dag:
    task_1 = DummyOperator(task_id = "first_task")
    task_2 = DummyOperator(task_id = "second_task")
    task_3 = DummyOperator(task_id = "third_task")
    task_4 = DummyOperator(task_id = "fourth_task")
    task_5 = DummyOperator(task_id = "fifth_task")

    task_1 >> task_2 >> task_3 >> task_4 >> task_5 
"""

# parallel split
"""
with new_dag:
    task_1 = DummyOperator(task_id = "first_task")
    task_2 = DummyOperator(task_id = "second_task")
    task_3 = DummyOperator(task_id = "third_task")
    task_4 = DummyOperator(task_id = "fourth_task")
    task_5 = DummyOperator(task_id = "fifth_task")

    task_1 >> task_2 >> [task_3, task_4] >> task_5
"""

# sycronitazions
"""
with new_dag:
    task_1 = DummyOperator(task_id = "first_task")
    task_2 = DummyOperator(task_id = "second_task")
    task_3 = DummyOperator(task_id = "third_task")
    task_4 = DummyOperator(task_id = "fourth_task")
    task_5 = DummyOperator(task_id = "fifth_task")
    task_6 = DummyOperator(task_id = "sixth_task")
    task_7 = DummyOperator(task_id = "seventh_task")
    final_task = DummyOperator(task_id = "final_task")

    task_1 >> task_2 >> [task_3, task_4, task_5, task_6, task_7] >> final_task
"""

# exculasive_choice
"""
with new_dag:
    task_1 = DummyOperator(task_id = "first_task")
    task_2 = DummyOperator(task_id = "second_task")
    task_3 = BranchPythonOperator(task_id = "third_task",callable=choice_function)
"""

# dynamic task generations (syncronization)

with new_dag:
    #dynamic generation
    final_task = DummyOperator(task_id = "final_task")
    for hour in range(0,10):
        import_task = DummyOperator(task_id='read_input_hour_{}'.format(hour))
        data_processing_task = DummyOperator(task_id='generate_data_hour_{}}'.format(hour))
        import_task >> data_processing_task >> final_task