import os
import sys
import configparser

from datetime import datetime
from validator import doValidate
from helper import getTablesWithPrimaryKey
from os.path import abspath, dirname, join

configFile= join(dirname(abspath(__file__)), 'config.ini')

config = configparser.ConfigParser()
config.read(configFile)

pathEntity = config['PATHS']['pathEntity']
pathSource=  config['PATHS']['pathSource']
primaryKeys= config['OTHERS']['primaryKeys'].split(",")
outputDirectory= config['OTHERS']['outputDirectory']
outputFileName= config['OTHERS']['defaultFileName']

def main(argv):
    start = datetime.now() 
    print("STARTED on %s" %start)

    tableDict= getTablesWithPrimaryKey(pathEntity, primaryKeys).keys() 
    tableList= list(tableDict)
    print("No. of tables identified: ", len(tableList))
    
    dirs = os.listdir( pathSource )
    for dir in dirs:
        if "." in dir:
            continue
        directory= os.path.join(pathSource, dir)
        outputFileLog= join(outputDirectory, '%s_%s.txt' %(outputFileName, dir))
        outputFileXls= join(outputDirectory, '%s_%s.xlsx' %(outputFileName, dir))
        print("directory: %s; outputFileLog: %s; outputFileXls: %s" %(directory, outputFileLog, outputFileXls))
        #doValidate(tableList, directory, primaryKeys, outputFileLog, outputFileXls)

    finish = datetime.now()
    print("ENDED on %s" %finish)
    print("Scan Duration: %s" %(finish - start))


if __name__ == "__main__":
    main(sys.argv[1:])