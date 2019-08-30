import os
import sys
import configparser

from datetime import datetime
from validator import doValidate
from helper import makeDirectory, getTablesWithPrimaryKey
from os.path import abspath, dirname, join

def main(argv):
	try: 
		configFile= join(dirname(abspath(__file__)), 'config.ini')
		config = configparser.ConfigParser()
		config.read(configFile)

		pathEntity = config['PATHS']['pathEntity']
		pathSource=  config['PATHS']['pathSource']
		primaryKeys= config['OTHERS']['primaryKeys'].split(",")
		outputDirectory= config['OTHERS']['outputDirectory']
		outputFileName= config['OTHERS']['outputFileName']
		
		start = datetime.now() 
		print("STARTED on %s" %start)

		tableDict= getTablesWithPrimaryKey(pathEntity, primaryKeys).keys() 
		tableList= list(tableDict)
		print("No. of tables identified: ", len(tableList))

		if not outputDirectory:
    			outputDirectory= join(dirname(abspath(__file__)), "output")
			
		makeDirectory(outputDirectory)
		print("Output folder: %s" %outputDirectory)
		outputFileLog= join(outputDirectory, '%s.txt' %outputFileName)
		outputFileXls= join(outputDirectory, '%s.xlsx' %outputFileName)
		doValidate(tableList, pathSource, primaryKeys, outputFileLog, outputFileXls)

		finish = datetime.now()
		print("ENDED on %s" %finish)
		print("Scan Duration: %s" %(finish - start))
	except Exception as ex:
		print('\nERROR FOUND!\n{}'.format(ex))
	finally:
		input("\nPress enter to exit!")
		
if __name__ == "__main__":
    print("Initializing...")
    main(sys.argv[1:])