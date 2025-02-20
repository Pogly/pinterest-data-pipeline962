from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

import yaml

# NOTE would add filepath the a configuration file.
with open("dagFilepath.Yaml") as Endpoint:
    invoke_url = yaml.safe_load(Endpoint)

notebook_task = {
    'notebook_path': Endpoint,
}

notebook_params = {
    "Variable":5
}

default_args = {
    'owner': 'BenG',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('e89446818119_dag',
    start_date= datetime(2025,2,6),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run