import logging
import os
from dotenv import load_dotenv
import datetime
import pyspark 
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import *

def initLogger(ASOFDATE='19900101'): 
    # print(os.getenv('LOGFILE'),str(ASOFDATE))
    logFileName = os.getenv('LOGFILE')+'_'+str(ASOFDATE)+'_'+datetime.datetime.now().strftime('%H-%M-%S')+'.log'
    logging.basicConfig(filename=logFileName )
    logging.root.setLevel(logging.INFO)
    # logging.basicConfig(level=logging.INFO)

    handle = 'Main Process '
    logger = logging.getLogger(handle)
    
    return logger

def initEnv(stage='DEV'):
    if stage=='PROD' : 
        load_dotenv('src\.env.prod')
    elif stage=='DEV' : 
        load_dotenv('src\.env.dev')
    else : 
        load_dotenv('src\.env.dev')

def getExecTime():
    d = datetime.datetime.now()
    ASOFDATE = str(d.strftime('%Y')+d.strftime("%m") +
                   f'{int(d.strftime("%d")):02d}')
    return(ASOFDATE)

def initSpark():
    spark = SparkSession.builder.master('local[1]').appName('Covid-dummy').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    return spark 

def readFile(spark , logger, filePath ) : 
    try : 
        df = spark.read.option('header',True).option('inferSchema',True).csv(filePath)
        return df 
    except Exception as ex : 
        logger.info('Error in reading file : '+ex)

