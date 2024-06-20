import logging
import os
from dotenv import load_dotenv
import datetime

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