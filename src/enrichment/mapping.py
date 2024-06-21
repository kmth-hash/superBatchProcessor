from helpers.utils import * 

def mainSparkProcess(spark , ASOFDATE , logger ): 
    df = readFile(spark , logger , 'src\enrichment\location-data.csv')
    df.show(10)

