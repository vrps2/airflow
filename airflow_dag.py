from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator. BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import boto3


def check_s3_file_count(ti):
    bucket_name='s3://'
    prefix= 'portal/radar_bulkupload/'
    s3=boto3.client('s3',region_name='us-east-1',endpoint_url='https://s3.us-east-1.amazonaws.com'
    paginator=s3.get_paginator('list_objects_v2')
    page_iterator=paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in page_iterator:
        if 'Contents' in page:
            key_count += len(page['Contents'])
    key_count = key_count -1

    ti.xcom_push(key='key_count',value=key_count)
    return key_count

def decide_which_path(ti):
    key_count=ti.xcom_pull(task_ids="check_s3_file_count",key="key_count")
    print("Key Count is ",key_count)
    if key_count >= 0:
        return "invoke_portal_lambda_running"
    else:
        return "end_workflow"

default_args= {'owner':'airflow'}

with DAG("WF_BULK_ADJ",
         start_date=days_ago(2),
        schedule_interval = None,
        default_args= default_args,
        render_template_as_native_obj=True,
        max_active_runs=1,
        tags=['ADJ']
         )as dag:

    tasks={}
    job_ids={





    }




