from helpers.utils import * 

def preprocess(spark , ASOFDATE , logger ):
    nameCsvFile = f"{os.getenv('INBOUND')}/{os.getenv('NAMESFILE')}"
    locationCsvFile=f"{os.getenv('INBOUND')}/{os.getenv('LOCATIONFILE')}"
    locationDF = readFile(spark , logger , locationCsvFile)
    locationDF.show()
    locationDF.printSchema()
    locationDF = locationDF.select(col("continent") , col("location")).withColumnRenamed("location" , "country")
    # locationDF.show()    
    locationDF=locationDF.filter(col('continent').isNotNull())
    

    userDF = readFile(spark , logger , nameCsvFile)
    userDF = userDF.select(col('Gender') , col("Child's First Name")).withColumnRenamed("Child's First Name",'userName')
    userDF.dropDuplicates(['userName'])
    logger.info(f'Total Users found : {userDF.count()}')
    userDF = userDF.dropDuplicates(['userName'])
    logger.info(f'Distinct Users found : {userDF.count()}')
    
    userDF = addStatustoDF(userDF)
    userDF = addStatustoDF(userDF) 
    userDF = addMedstoDF(userDF)
    userDF = addMedCounttoDF(userDF)
    
    # userDF.show()
    userDF = userDF.withColumn("vaccineStatus" , when(col('medCount')>0 , "Vaccinated").otherwise("Not Vaccinated"))
    
    sampleDF = locationDF.crossJoin(userDF).sample(0.2)
    
    # preprocessDF = sampleDF.groupBy('country').pivot('med').count()
    # preprocessDF = sampleDF.join(preprocessDF , preprocessDF['country']==sampleDF['country'] , 'left')
    # preprocessDF.show()



    return sampleDF 


def countryStats(spark , logger , ASOFDATE , srcDF ) : 
    logger.info("Source Data loaded : ")
    logger.info(f"Total Source count for users : {srcDF.count()}")
    
    groupedDF = srcDF.groupby("country").pivot('status').count()

    vacGroupDF = srcDF.groupby('country').pivot('vaccineStatus').count()
    vacGroupDF = vacGroupDF.withColumnRenamed('country' , 'vacCountry')
    
    srcDF = srcDF.drop("Gender" , "userName")
    srcDF = srcDF.withColumnRenamed('country' , 'srcCountry')
    srcDF = srcDF.dropDuplicates(['srcCountry'])
    
    srcDF = srcDF.join(groupedDF , groupedDF['country']==srcDF['srcCountry'] , 'right').join(vacGroupDF , vacGroupDF['vacCountry']==srcDF['srcCountry'],'left' )
        
    srcDF = srcDF.distinct().drop('medCount' , 'srcCountry', 'vacCountry','med' , 'status','vaccineStatus')
    logger.info(f'Distinct records found : {srcDF.count()}')
    logger.info(srcDF.collect()[5])
    # srcDF.show(5)
    
    return srcDF 
 


def mainSparkProcess(spark , ASOFDATE , logger ): 

    srcDir = f"{os.getenv('OUTBOUNDTMP')}"
    destDir = f"{os.getenv('OUTBOUND')}"
    destFileName = os.getenv('FINALFILE')
    

    cleanedDF = preprocess(spark , ASOFDATE , logger )
    finalDF1 = countryStats(spark , logger , ASOFDATE , cleanedDF )
    
    deleteData(spark , logger , srcDir)
    finalDF1.coalesce(1).write.option("header","true").csv(f"{srcDir}")

    tmpFileCleanUp(spark , logger , srcDir, destDir, destFileName)


