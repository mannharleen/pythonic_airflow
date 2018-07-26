from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.models import Variable

START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
DAG_NAME = 'dag3_emr_hook'
# Use airflow env variables
ENV = Variable.get("ENV", default_var="DEV")
HIVEPASSWORD = Variable.get("HIVEPASSWORD", default_var="DEV")      # should ideally be dev.hivepassword !!!

print("obtained ENV as {}".format(ENV))

# initialize the DAG
default_args = {
    'pool': 'emr_model_building',
    'depends_on_past': False,
    'start_date': START,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='0 1 * * *')


def create_emr():
    conn = EmrHook(emr_conn_id='emr_default')
    response = conn.create_job_flow()
    print (response['JobFlowId'])
    return response


t1 = PythonOperator(
        task_id='t1_create_emr',
        python_callable=create_emr,
        dag=dag)

t1
