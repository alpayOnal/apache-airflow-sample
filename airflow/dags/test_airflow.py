from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import  logging

# import boto3
#
# from aws_hook import _get_credentials
# from airflow.contrib.hooks.aws_hook import AwsHook

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

dag = DAG('airflow_test_dag',
          max_active_runs=1,
          schedule_interval='0 * * * *',  # '@daily' '@weekly' ...
          # schedule_interval=timedelta(hours=24),
          default_args=default_args,
          description="test airflow")


# s3 = _get_credentials().resource('s3')
# s3.Bucket(bucket_name).upload_file(filename, key)


def start_AWS(ds, **kwargs):
    logging.info('--------------'+kwargs['name'])

    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")


def upload_file_to_S3(ds, **kwargs):
    logging.info('--------------'+kwargs['name'])
    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")


def write_context(ds, **kwargs):
    logging.info('--------------'+kwargs['name'])
    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")

#
# start_aws = PythonOperator(
#     task_id='start_AWS',
#     provide_context=True,
#     python_callable=write_context,
#     op_kwargs={'name': 'start_AWS'},
#     dag=dag,
# )

upload_file_to_s3 = PythonOperator(
    task_id='upload_file_to_S3',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'upload_file_to_S3'},
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

# task1.set_upstream(start_aws)
task2.set_upstream(task1)
task3.set_upstream(task1)
task4.set_upstream(task1)

task5 = PythonOperator(
    task_id='task5',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task5'},
    dag=dag,
)

task6 = PythonOperator(
    task_id='task6',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task6'},
    dag=dag,
)

task7 = PythonOperator(
    task_id='task7',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task7'},
    dag=dag,
)

task8 = PythonOperator(
    task_id='task8',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task8'},
    dag=dag,
)

task4.set_downstream([task5, task6, task7])

task9 = PythonOperator(
    task_id='task9',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task9'},
    dag=dag,
)

task10 = PythonOperator(
    task_id='task10',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task10'},
    dag=dag,
)

task11 = PythonOperator(
    task_id='task11',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task11'},
    dag=dag,
)

task12 = PythonOperator(
    task_id='task12',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task12'},
    dag=dag,
)

task13 = PythonOperator(
    task_id='task13',
    provide_context=True,
    python_callable=write_context,
    op_kwargs={'name': 'task13'},
    dag=dag,
)

task8.set_upstream([task5, task6, task7])
# task5.set_downstream(task8)
# task6.set_downstream(task8)
# task7.set_downstream(task8)

task9.set_upstream(task8)
task10.set_upstream(task9)
task11.set_upstream(task10)
task12.set_upstream(task11)
task13.set_upstream(task12)
upload_file_to_s3.set_upstream(task13)
