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


# Get hadoop instance 
# Instanciate File system 
def configure_hadoop(spark):
    hadoop = spark.sparkContext._jvm.org.apache.hadoop  
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)
    return hadoop, conf, fs 


# List all files in the Directory 
def list_files(spark , srDir ) : 
    hadoop , conf , fs = configure_hadoop(spark )
    files = [] 

    for fileItr in fs.listStatus(hadoop.fs.Path(srDir)) : 
        if fileItr.isFile() : 
            # print(fileItr.getPath(),end='\n\n')
            files.append(fileItr.getPath())
    if not files : 
        print("No files found in Directory ")
    return files


def create_Dir(spark ,fileDir) : 
    hadoop, _, fs = configure_hadoop(spark)
    if not fs.exists(hadoop.fs.Path(fileDir)):
        print(hadoop.fs.Path(fileDir))
        fs.mkdirs(hadoop.fs.Path(fileDir))
        # out_stream.close() 

def create_file(spark ,filename) : 
    hadoop, _, fs = configure_hadoop(spark)
    if not fs.exists(hadoop.fs.Path(filename)):
        print(hadoop.fs.Path(filename))
        response = fs.create(hadoop.fs.Path(filename))
        response.close() 

def deleteData(spark , logger , location) : 
    try:
        hadoop , _ , fs = configure_hadoop(spark)
        if fs.exists(hadoop.fs.Path(location)) : 
            fs.delete(hadoop.fs.Path(location))
    except:
        logger.info('Error in Delete operation. ')

def copyFileToDest(spark , logger , srcLoc , destLoc , fileName) : 
    try:
        hadoop , conf , fs = configure_hadoop(spark) 
        dataIn = fs.open(hadoop.fs.Path(srcLoc))
        dataOut = fs.create(hadoop.fs.Path(destLoc+'/'+fileName)) 
        hadoop.io.IOUtils.copyBytes(dataIn, dataOut, conf, False)
        logger.info("File data completed successfully.")

        
    except:
        logger.info('Error in copying data ')
    finally:
        dataOut.close()

    


