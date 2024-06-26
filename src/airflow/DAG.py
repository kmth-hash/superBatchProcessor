# Copy this file into your /airflow/dags directory
# Copy .env and .ready file into the same directory 
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow import DAG
from airflow.sensors.filesystem import FileSensor 
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
from dotenv import load_dotenv

#Place the same project env file in this directory 
load_dotenv('.env')

default_args = {
    'owner': 'kmth',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def getExecTime():
    d = datetime.now()
    ASOFDATE = str(d.strftime('%Y')+d.strftime("%m") +
                   f'{int(d.strftime("%d")):02d}')
    return(ASOFDATE)

def printTimeStamp(**kwargs ):
    d = datetime.now()
    print(f"{kwargs['params']['processName']} {kwargs['params']['status']} --> {d.strftime('%c')}")

def createReadyFile() : 
    readyFile= f"{os.getenv("S3READYLOCATION")}"
    awsRegion = f"{os.getenv("REGION")}"
    fileName =  f'/.ready'
    bucketName = f"{os.getenv("BUCKETID")}"
    client = boto3.client('s3' , region_name=awsRegion)
    client.upload_file(fileName , bucketName , readyFile)
    print('Ready file loaded ')

def loadMethod() : 
    fileName =  f"{os.getenv('OUTBOUND')}/{os.getenv("FINALFILE")}"
    bucketName = f"{os.getenv("BUCKETID")}"
    destinationPath = f"{os.getenv("BUCKETPATH")}"
    client = boto3.client('s3' , region_name=f"{os.getenv("REGION")}")
    client.upload_file(fileName , bucketName , destinationPath)
    print('File uploaded into S3')

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
                    'AttributeName': 'country',
                    'AttributeType': 'S',
                } 
                
            ] , 
            KeySchema=[
                {
                    'AttributeName': 'country',
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



with DAG('SBP1', default_args=default_args, schedule= '@once') as dag : 
    ASOFDATE = getExecTime()
    LogCounter = 1
    f1 = FileSensor(filepath=f"{os.getenv("INBOUND")}/{os.getenv("NAMESFILE")}" ,
                    task_id='namesensor' , 
                    poke_interval = 30 , 
                    dag=dag )

    f2 = FileSensor(filepath=f"{os.getenv("INBOUND")}/{os.getenv("LOCATIONFILE")}" ,
                    task_id='locationsensor' , 
                    poke_interval = 30 , 
                    dag=dag)

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
        bash_command='echo Last task shutting down $(date);' , 
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
    
    sparkProcess = BashOperator(
        dag = dag ,
        bash_command=f"{os.getenv("UNIXDIR")}/main_process.py",
        task_id='python_spark_proc' , 
        output_encoding="utf-8" 
    )

    pyLog = PythonOperator(
        task_id=f'py_log_{LogCounter}' , 
        dag=dag , 
        python_callable=printTimeStamp , 
        params={'status' : 'Started' , 'processName' : 'S3 Bucket Creation '},
    )

    LogCounter += 1 

    loadOutputIntoS3 = PythonOperator(
        task_id='load_data_into_s3' , 
        dag = dag ,
        python_callable=loadMethod , 
        params={'ASOFDATE' : '20240625' , 'processName' : 'Load data into s3'},
    )

    loadReadyIntoS3 = PythonOperator(
        dag =dag ,
        task_id = 'ready_file_created' ,
        python_callable=createReadyFile , 

    )

 
    maintask >> [f1 , f2] >> maintask >> [t1 , checkDB ]
    t1 >>create_bucket >> t2 
    [checkDB , t2] >> t3 >> pyLog
    pyLog >> sparkProcess >>loadOutputIntoS3 
    loadOutputIntoS3 >> loadReadyIntoS3