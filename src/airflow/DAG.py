# DAG to create S3 bucket 
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow import DAG
from airflow.operators.python  import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv('src\.env.dev')


default_args = {
    'owner': 'kmth',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def printTimeStamp(**kwargs ):
    d = datetime.now()
    print(f"{kwargs['params']['processName']} {kwargs['params']['status']} --> {d.strftime('%c')}")


dag = DAG('S3_CREATE_DAG', default_args=default_args, schedule= '@once')

t1 = PythonOperator(
    task_id='logger-start',
    python_callable=printTimeStamp,
    params={'status' : 'Started' , 'processName' : 'S3 Bucket Creation '},
    dag=dag)

create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        region_name=os.getenv('BUCKETREGION'),
        bucket_name=os.getenv('BUCKETID'),
        aws_conn_id='S3_conn',
    )
t2 = PythonOperator(
    task_id='logger-end',
    python_callable=printTimeStamp,
    params={'status' : 'Ended' , 'processName' : 'S3 Bucket Creation '},
    dag=dag)

t1 >>create_bucket >> t2 
