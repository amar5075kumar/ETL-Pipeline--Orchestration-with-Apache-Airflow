from airflow import DAG
import boto3
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from time import sleep
from airflow import DAG
import boto3
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from time import sleep
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator
## Build the connection For AWS on Airflow
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html
import os
from airflow.models.connection import Connection
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta

conn = Connection(
    conn_id="AWS_Connection",
    conn_type="aws",
    login="***********************",  # Reference to AWS Access Key ID
    password="*************************************",  # Reference to AWS Secret Access Key
    extra={
        # Specify extra parameters here
        "region_name": "us-east-1",   ## Select the region in which the jobs are been running 
    },
)


# Initialize Glue client
aws_region = "us-east-1"   ## Select the region in which the jobs are been running 

# Define the AWS Access Key ID and Secret Access Key
aws_access_key_id = "********************************"
aws_secret_access_key = "************************************"

# Set AWS CLI environment variables
os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
os.environ["AWS_DEFAULT_REGION"] = aws_region 

# Generate Environment Variable Name and Connection URI
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
print(f"{env_key}={conn_uri}")

# Define your AWS region and Glue job names
# glue job environment variable
glue_s3_folder = "s3://***********************"
glue_job_name = ['JOB1','JOB2','JOB3','JOB4','JOB5','JOB6','JOB7','JOB8']

role_name='*************'


# initialize glue client
glue = boto3.client('glue',region_name='us-east-1')

default_args = {
    'owner': 'Amar',
    'start_date': datetime(2023, 9, 19),        
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    dag_id='glue_pipeline',
    description='Run Glue Pipeline',
    start_date=datetime(2023, 9, 15),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)

start_task = EmptyOperator(task_id='start', dag=dag)
# Use a EmptyOperator as the ending  point
end_task = EmptyOperator(task_id='end', dag=dag)

glue_job_tasks = {}

for job_name in glue_job_name:
    task_id=f"submit_glue_job_{job_name}"
    if task_id not in glue_job_tasks:
        glue_job_task = GlueJobOperator(
            task_id=f"submit_glue_job_{job_name}",
            job_name=job_name,
            script_location=f"s3://{glue_s3_folder}/{job_name}.py",
            s3_bucket='**********', ## Specify the s3_bucket name 
            region_name='us-east-1',
            aws_conn_id=None,
            iam_role_name=role_name,
            wait_for_completion=True,
            create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
            dag=dag,
        )
        glue_job_tasks[job_name] = glue_job_task
        # print(glue_job_tasks)

    else:
        print(f"Task with task id {task_id} is already present ")

# Use a EmptyOperator as the ending  point
# end_task = EmptyOperator(task_id='end', dag=dag)

wait_time = timedelta(minutes=1)
wait_sensor_d={}
for i in range(len(glue_job_name)//2):
    wait_sensor = TimeDeltaSensor(
    task_id=f'wait_sensor_{i}',
    delta=wait_time,
    dag=dag,
    )
    wait_sensor_d[i]=wait_sensor


start_task >> glue_job_tasks['JOB1']
glue_job_tasks['JOB1']>>wait_sensor_d[0]>>[glue_job_tasks['JOB2'],glue_job_tasks['JOB3']]
glue_job_tasks['JOB3']>>wait_sensor_d[1]>>[glue_job_tasks['JOB4'],glue_job_tasks['JOB5']]
glue_job_tasks['JOB5']>>wait_sensor_d[2]>>[glue_job_tasks['JOB6'],glue_job_tasks['JOB7']]
glue_job_tasks['JOB7']>>wait_sensor_d[3]>>glue_job_tasks['JOB8']
glue_job_tasks['JOB8']>>end_task
  






