import logging
import os
from dotenv import load_dotenv
import datetime
import findspark
findspark.init()

from random import randint , choice 

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
        load_dotenv('src/.env.prod')
    elif stage=='DEV' : 
        load_dotenv('src/.env.dev')
    else : 
        load_dotenv('src/.env.dev')

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


def addStatustoDF(userDF) : 
    
    def generateStatus(z):
        return str(choice(['Dead' , 'Recovered' , 'Affected']))
    
    statusUDF = udf(lambda z : generateStatus(z) , StringType())
    userDF = userDF.withColumn('status' , statusUDF(col('userName')) )
    return userDF

def addAgetoDF(userDF) : 
    def generateAge(z):
        return str(randint(5,85))

    ageDF = udf(lambda z: generateAge(z) , StringType())
    userDF = userDF.withColumn('age' , ageDF(col('userName')))
    return userDF 

def addMedstoDF(userDF) : 
    def generateMeds(z):
        return choice(['Covaxin', 'Covishield', 'Sputnik'])

    medsDF = udf(lambda z: generateMeds(z) , StringType()) 
    userDF = userDF.withColumn('med' , medsDF(col('userName')))
    return userDF 


def addMedCounttoDF(userDF) : 
    def generateCount(z):
        return str(choice(['0' , '1' , '2']))

    ageDF = udf(lambda z: generateCount(z) , StringType())
    userDF = userDF.withColumn('medCount' , ageDF(col('userName')))
    return userDF