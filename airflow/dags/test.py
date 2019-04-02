from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'test_airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG('airflow_test_dag',
          max_active_runs=1,
          schedule_interval='0 * * * *',
          # schedule_interval=timedelta(hours=24),
          default_args=default_args,
          description="test airflow")


def write_context(ds, **kwargs):
    with open("/app/test_airflow.log", "a") as myfile:
        myfile.write(kwargs['name'] + '|' + str(ds) + '|' + str(datetime.now()) + "\n")


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

task5.set_upstream(task4)
task6.set_upstream(task4)
task7.set_upstream(task4)
task8.set_upstream(task4)

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

task9.set_upstream(task6)
task10.set_upstream(task6)
task11.set_upstream(task6)
task12.set_upstream(task6)
task13.set_upstream(task6)
