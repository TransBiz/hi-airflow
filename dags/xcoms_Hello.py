import airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}
def dolist(**kwargs):
    return 'world'

def print_hello(**kwargs):
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='hello_list')
    return 'Hello' + value

dag = DAG('hello_world_list', description='Simple tutorial DAG',
        schedule_interval='0 12 * * *',
        start_date=datetime(2017, 3, 20), catchup=False,
    default_args=args)

list_operator = PythonOperator(task_id='hello_list', python_callable=dolist, dag=dag)
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
list_operator >> hello_operator

