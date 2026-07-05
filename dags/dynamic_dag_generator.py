import yaml
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from tasks.fetch_api import fetch_data
from tasks.quality_check import data_quality_check
from tasks.state_evaluator import evaluate_business_logic
from tasks.db_writer import write_to_postgres
from tasks.email_sender import send_email

CONFIG_PATH = "/opt/airflow/config/trackers.yaml"

def branch_email_check(**kwargs):
    ti = kwargs['ti']
    email_needed = ti.xcom_pull(task_ids='evaluate_business_logic', key='email_needed')
    if email_needed:
        return 'send_email'
    return 'end'

def create_dag(dag_config):
    dag_id = dag_config['dag_id']
    schedule_interval = dag_config['schedule']
    
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'execution_timeout': timedelta(minutes=1)
    }
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        dagrun_timeout=timedelta(minutes=1),
        is_paused_upon_creation=False
    )
    
    with dag:
        start = EmptyOperator(task_id='start')
        
        fetch_data_task = PythonOperator(
            task_id='fetch_data',
            python_callable=fetch_data,
            op_kwargs={'config': dag_config}
        )
        
        data_quality_check_task = PythonOperator(
            task_id='data_quality_check',
            python_callable=data_quality_check,
            op_kwargs={'config': dag_config}
        )
        
        evaluate_business_logic_task = PythonOperator(
            task_id='evaluate_business_logic',
            python_callable=evaluate_business_logic,
            op_kwargs={'config': dag_config}
        )
        
        write_to_postgres_task = PythonOperator(
            task_id='write_to_postgres',
            python_callable=write_to_postgres,
            op_kwargs={'config': dag_config}
        )
        
        branch_email_check_task = BranchPythonOperator(
            task_id='branch_email_check',
            python_callable=branch_email_check,
        )
        
        send_email_task = PythonOperator(
            task_id='send_email',
            python_callable=send_email,
            op_kwargs={'config': dag_config}
        )
        
        end = EmptyOperator(task_id='end')
        
        start >> fetch_data_task >> data_quality_check_task >> evaluate_business_logic_task >> write_to_postgres_task >> branch_email_check_task
        branch_email_check_task >> send_email_task >> end
        branch_email_check_task >> end
        
    return dag

if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, 'r') as file:
        config_data = yaml.safe_load(file)
        for tracker in config_data.get('trackers', []):
            globals()[tracker['dag_id']] = create_dag(tracker)
