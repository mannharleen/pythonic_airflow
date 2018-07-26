from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable

START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
DAG_NAME = 'emr_model_building'
#ENV = 'DEV'
ENV = Variable.get("ENV", default_var="DEV")
HIVEPASSWORD = Variable.get("HIVEPASSWORD", default_var="DEV")

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

# define the bash commands used in the tasks
launch_emr = """
 {% if params.ENV == "PROD" %}
 echo "Launching EMR cluster in Prod Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh launch,provision,deploy model_building_prod.conf
 {% else %} 
 echo "Launching EMR cluster in Non prod Env"
 HIVEPASSWORD = {{ params.HIVEPASSWORD }} 
 /home/ubuntu/airflow/pythonic_airflow/dags/dag2_start_emr.sh
 {% endif %}
 """

run_sm_and_reputation = """
 {% if params.ENV == "PROD" %}
 echo "Building sender models in Prod Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh sd model_building_prod.conf
 {% else %}
 echo "Building sender models in Stage Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh sd model_building_stage.conf
 {% endif %}
 """

run_cdd = """
 {% if params.ENV == "PROD" %}
 echo "Building CDD in Prod Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh cdd model_building_prod.conf
 {% else %}
 echo "Building CDD in Stage Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh cdd model_building_stage.conf
 {% endif %}
 """

terminate_cluster = """
 {% if params.import_terminate_emr_cluster == true %}
 {% if params.ENV == "PROD" %}
 echo "Terminating EMR cluster in Prod Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh terminate model_building_prod.conf
 {% else %}
 echo "Terminating EMR cluster in Stage Env"
 source ~/.bash_profile; /home/deploy/automation/roles/cluster/cluster.sh terminate model_building_stage.conf
 {% endif %}
 {% else %}
 echo "NOT terminating EMR cluster"
 {% endif %}
 """

# define the individual tasks using Operators
# t0 = ExternalTaskSensor(
#     task_id='wait_for_previous_run',
#     trigger_rule='one_success',
#     external_dag_id=DAG_NAME,
#     external_task_id='terminate_cluster',
#     allowed_states=['success'],
#     execution_delta=timedelta(days=1),
#     dag=dag)

t1 = BashOperator(
    task_id='launch_emr',
    bash_command=launch_emr,
    #bash_command="aws s3 ls",
    execution_timeout=timedelta(hours=6),
    pool='emr_model_building',
    params={'ENV': ENV, 'HIVEPASSWORD': HIVEPASSWORD},
    dag=dag)

# t2 = BashOperator(
#     task_id='run_sm_and_reputation',
#     bash_command=run_sm_and_reputation,
#     execution_timeout=timedelta(hours=3),
#     pool='emr_model_building',
#     params={'ENV': ENV},
#     dag=dag)
#
# t3 = BashOperator(
#     task_id='run_cdd',
#     bash_command=run_cdd,
#     execution_timeout=timedelta(hours=3),
#     pool='emr_model_building',
#     params={'ENV': ENV},
#     dag=dag)
#
# t4 = BashOperator(
#     task_id='terminate_cluster',
#     bash_command=terminate_cluster,
#     execution_timeout=timedelta(hours=1),
#     params={'ENV': ENV},
#     pool='emr_model_building',
#     dag=dag)

# construct the DAG
t1