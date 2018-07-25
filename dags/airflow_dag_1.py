from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from tasks import pytask1


default_args = {
    'owner': 'mannh',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 1),
    #'end_date': datetime(2015, 6, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('my_dag_id_1', default_args=default_args, schedule_interval=timedelta(hours=12), catchup=False)

#task t1
t1 = BashOperator(task_id='t1', bash_command='date', dag=dag)

#task t2
t2 = PythonOperator(task_id='t2', python_callable=pytask1.pytask1_main, dag=dag)

#task t3
t1 >> t2
