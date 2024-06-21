from helpers.utils import *
from enrichment.mapping import * 
import datetime


def main():
    ASOFDATE = getExecTime()
    initEnv('DEV')
    logger = initLogger(ASOFDATE)

    logger.info(f'Start ETL Process : {datetime.datetime.now().strftime("%c")}')
    
    spark = initSpark()

    mainSparkProcess(spark , ASOFDATE , logger)


if __name__ == "__main__":
    main()
