from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

import boto3

default_args = {
    'owner': 'test_airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 1),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG('airflow_aws_test_dag',
          max_active_runs=1,
          schedule_interval='0 * * * *',  # '@daily' '@weekly' ...
          # schedule_interval=timedelta(hours=24),
          default_args=default_args,
          description="test airflow")


def start_aws_instance(ds, **kwargs):
    logging.info('--------------' + kwargs['name'])
    # ec2 = boto3.client('ec2', region_name='us-east-2')
    # ec2.start_instances(InstanceIds=['i-066ded0ad509ccdd3'])

    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")


def stop_aws_instance(ds, **kwargs):
    logging.info('--------------' + kwargs['name'])
    # ec2 = boto3.client('ec2', region_name='us-east-2')
    # ec2.stop_instances(InstanceIds=['i-066ded0ad509ccdd3'])

    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")


def write_context(ds, **kwargs):
    logging.info('--------------' + kwargs['name'])
    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")


start_aws = PythonOperator(
    task_id='start_AWS',
    provide_context=True,
    python_callable=start_aws_instance,
    op_kwargs={'name': 'start_AWS'},
    dag=dag,
)

stop_aws = PythonOperator(
    task_id='stop_aws',
    provide_context=True,
    python_callable=stop_aws_instance,
    op_kwargs={'name': 'stop_AWS'},
    dag=dag,
)

task1 = PythonOperator(
    task_id='task1',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task1'},
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task2'},
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task3'},
    dag=dag,
)

task4 = PythonOperator(
    task_id='task4',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task4'},
    dag=dag,
)

task1.set_upstream(start_aws)
task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)

stop_aws.set_upstream(task4)
