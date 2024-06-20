from helpers.utils import *
import datetime


def main():
    ASOFDATE = getExecTime()
    initEnv('DEV')
    logger = initLogger(ASOFDATE)

    logger.info(f'Start ETL Process : {datetime.datetime.now().strftime("%c")}')
    


if __name__ == "__main__":
    main()
