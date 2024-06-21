import logging 
import boto3 
import io 
import os 
import pandas as pd 

def readFileFromS3(logger , bucketName , fileName , region ) : 
    s3conn = boto3.client('s3' , region)
    logger.info(f"Read data from {fileName}")
    body = s3conn.get_object(Bucket=bucketName , Key=fileName)    
    data = pd.read_csv(io.BytesIO(body['Body'].read()))
    return data

def loadtoDB(logger , tableName , fileData, region ):
    logger.info('Connecting to DB ')
    db = boto3.resource('dynamodb' , region_name=region).Table(tableName)
    # print(type(fileData))
    # logger.info(db , fileData)
    logger.info('Loading data into DynamoDB ')
    with db.batch_writer() as bw : 
        for i , row in fileData.iterrows() : 
            d = row.to_dict()
            bw.put_item(Item=d)
    logger.info("Data load Complete ")
        

def lambda_handler(event,context) : 
    logging.basicConfig()
    logging.root.setLevel(logging.INFO)
    regionName = os.getenv('REGION')
    fileName = os.getenv('FILENAME')
    bucketName = os.getenv('BUCKETID')
    dbName = os.getenv('DYNAMODBTABLE')
    
    handle = 'LambdaLoader'
    logger = logging.getLogger(handle)
    logger.info([regionName , bucketName , fileName , dbName])
    logger.info('Handler Function Invoked : .ready file found ')
    # logger.info(f'Logs stream : {context['log_stream_name']}')
    data = readFileFromS3(logger, bucketName , fileName , regionName  )
    loadtoDB(logger , dbName , data,regionName)
