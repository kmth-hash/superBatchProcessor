from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
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

def checkDBExists(**kwargs ): 
    d = datetime.now() 
    print(f"{kwargs['params']['processName']} Started --> {d.strftime('%c')}")
    client = boto3.client('dynamodb')
    tableName = os.getenv('DYNAMODBTABLE')
    print(os.getcwd() , tableName , os.getenv('BUCKETREGION'))
    try:
        response = client.describe_table(TableName=tableName)
        # print(response)
        print('DynamoDB Table exists. ')
    except client.exceptions.ResourceNotFoundException as ex :
        print('Table not found. ')
        response = client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'ID',
                    'AttributeType': 'S',
                } 
                
            ] , 
            KeySchema=[
                {
                    'AttributeName': 'ID',
                    'KeyType': 'HASH',
                } 
            ] ,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
            },
            TableName=tableName,
        )
        # print(response)
    finally : 
        d = datetime.now() 
        print(f"{kwargs['params']['processName']} Ended --> {d.strftime('%c')}")



dag = DAG('Main_Process_DAG', default_args=default_args, schedule= '@once')

t1 = PythonOperator(
    task_id='logger-s3-start',
    python_callable=printTimeStamp,
    params={'status' : 'Started' , 'processName' : 'S3 Bucket Creation '},
    dag=dag)

t2 = PythonOperator(
    task_id='logger-s3-end',
    python_callable=printTimeStamp,
    params={'status' : 'Ended' , 'processName' : 'S3 Bucket End '},
    dag=dag)

t3 = BashOperator(
    task_id='EndTask' , 
    bash_command='echo Last task shutting down ==> $(date);' , 
    output_encoding="utf-8" , 
    dag=dag
)

maintask = BashOperator(
    task_id='FinalTaskInit' , 
    bash_command='echo Main DAG executed $(date);' , 
    output_encoding="utf-8" , 
    dag=dag
)

create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        region_name=os.getenv('BUCKETREGION'),
        bucket_name=os.getenv('BUCKETID'),
        aws_conn_id='S3_conn',
    )

checkDB = PythonOperator(
    task_id='DB-exists',
    python_callable=checkDBExists,
    params={'processName' : 'DynamoDB Checker'},
    dag=dag
)
maintask >> [t1 , checkDB ]
t1 >>create_bucket >> t2 
[checkDB , t2] >> t3
