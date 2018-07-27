from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.models import Variable

START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)

DAG_NAME = 'dag4_multi_bash'
ENV = Variable.get("ENV", default_var="DEV")

# initialize the DAG
default_args = {
    'pool': 'pool4',
    'depends_on_past': False,
    'start_date': START,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='0 1 * * *', catchup=False)

# t1: get absolute path of current location
t1 = BashOperator(
        task_id='t1_create_emr',
        dag=dag,
        bash_command='pwd',
        xcom_push=True
    )

# t2: list the files using the abs path from t1
t2 = BashOperator(
        task_id='t1_create_emr',
        dag=dag,
        bash_command= "ls {{ti.xcom_pull(task_ids='t1_create_emr')}}",
        #xcom_push=True
    )

t1 >> t2