"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""

import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator

default_args = {
    "owner": "databricks",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 20),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'db_projects_dag',
    default_args=default_args,
    description='A Databricks DAG show projects API',
    schedule_interval=None,
    catchup=False
)

# 

api = "/api/2.0/projects/fetch-and-checkout"
databricks_conn_id = 'databricks_demo'
token = BaseHook.get_connection(databricks_conn_id).extra_dejson['token']

fetch_and_checkout_op = SimpleHttpOperator(
    task_id=f'fetch_and_checkout_op',
    # host is https://<name>.cloud.databricks.com, everything else empty in connection
    http_conn_id='http_databricks',
    method='POST',
    endpoint=api,
    data=json.dumps({"branch": "dev", "path": "/Projects/viren.patel@databricks.com/databricks-cicd"}),
    headers={'Authorization': 'Bearer %s' % token, 'Content-Type': 'application/json'},
    dag=dag,
)

notebook_run = DatabricksRunNowOperator(
    task_id=f'notebook_A', 
    dag=dag, 
    databricks_conn_id=databricks_conn_id, 
    job_id = 38245,
    )

fetch_and_checkout_op >> notebook_run
