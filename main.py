import os
import re

TABLE_TAG= "_TBL"
DOT_TAG= "."
SPACE_TAG= " "
AS_TAG= "AS"
ON_TAG= "ON"
NEWLINE= "\n"
TABLE_NAME_INDEX= 2

primaryKeys= ["KAISHA_CD"]
primaryKeyIndicator= "primaryKeyMap.put"
physicalTableName= "DatabaseTableInfo"

entityPath = r"path_of_the_table_entity"
filesPath= r"path_of_your_files_to_validate"
outputFile= r"output.txt"


def getFiles(path, searchPattern=".xml"):
    files = []
    fileNames=[]
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            #print("File: ", file)
            if searchPattern in file:
                files.append(os.path.join(r, file))
                fileNames.append(file)
    return files, fileNames


def getTablesWithPrimaryKey(path, primaryKeysToCheck, fileNamePattern= "_TBL.java"):
    files, fileNames= getFiles(path, fileNamePattern)
    primaryKeysPattern= "|".join(primaryKeysToCheck)
    result= {}

    for file in files:
        with open(file, 'rt', encoding='utf-8') as fp:
            #print("File: ", file)
            tableName= None
            pkList= []

            for line in fp:
                if physicalTableName in line:
                    itemList= re.sub(r"[^\x00-\x7F]|\W+", SPACE_TAG, line).strip().upper().split(SPACE_TAG)
                    tableName= itemList[TABLE_NAME_INDEX]
                elif primaryKeyIndicator in line:
                    pk= re.findall(primaryKeysPattern, line, re.IGNORECASE)
                    if pk:
                        pkList.append(pk[0])
            if tableName and pkList:
                result[tableName]= pkList
    return result


def getTableInfo(line):
    tableName= None
    tableAlias= None
    #print("> Line: ", line)

    if DOT_TAG not in line and TABLE_TAG in line:
        newLine= re.sub(r"[^\x00-\x7F]|\W+", SPACE_TAG, line).strip().upper()
        #print("newLine: ", newLine)

        index= re.search("\w+_TBL", newLine)
        splitResult= newLine[index.start():].split(SPACE_TAG)        
        tableName= splitResult[0]
        #print("splitResult: ", splitResult)
        #print("tableName: ", tableName)

        if len(splitResult)>= 2:
            if splitResult[1] == ON_TAG:
                tableAlias= tableName
            elif splitResult[1] == AS_TAG and len(splitResult) >= 3:
                tableAlias= splitResult[2]
            else:
                tableAlias= splitResult[1]
        else:
            tableAlias= tableName

    #print("tableName is %s, alias is %s" %(tableName, tableAlias))
    return tableName, tableAlias


def saveFindings(fileName, itemList):
    with open(fileName, 'w', encoding='utf-8') as fp:
        print("Writing feedback...")
        feedback= "No errors found!"
        if itemList:
            print(">>> Failed:")
            feedback= NEWLINE.join(itemList)
        else:
            print(">>> Successful")
        fp.write(feedback)


def main(tableList, searchPath):
    print("STARTING...")
    xmlFiles, fileNames= getFiles(searchPath)
    
    for file in xmlFiles:
        findingsList= []

        with open(file, 'rt', encoding='utf-8') as fp:
            #print("File: %s" %file)
                
            lineNo= 0
            patternList=[]
            physicalTableList=[]
            tableAliasList=[]
            tableLineNoIndicator= {}
            
            for line in fp:
                lineNo+= 1
                tableName, tableAlias= getTableInfo(line)
                #print(line)
                #print(">> tableName is %s, alias is %s" %(tableName, tableAlias))
                
                if tableName is not None and tableName in tableList:
                    #print("tableName is %s, alias is %s" %(tableName, tableAlias))
                    for pk in primaryKeys:
                        tableAliasWithPk= "%s.%s" %(tableAlias,pk)

                        if tableAliasWithPk not in tableAliasList:
                            physicalTableList.append(tableName)
                            tableAliasList.append(tableAliasWithPk)
                            tableLineNoIndicator[tableAlias]= lineNo
                            patternList.append(r"\b%s\b" %tableAliasWithPk)

                    #print("physicalTableList: ", physicalTableList)
                    #print("patternList: ", patternList)
                    #print("tableAliasList: ", tableAliasList)

                elif DOT_TAG in line:
                    #print("patternList: ", patternList)
                    #print("Line no. %d: %s" %(lineNo, line))
                    if patternList:
                        searchPatten= '|'.join(patternList)
                        try:
                            #print("searchPatten: ", searchPatten)
                            regex= re.compile(searchPatten, re.IGNORECASE)
                            resultList = regex.findall(line)
                            #print(">> resultList: ", resultList)
                        except:
                            print("> Exception occurred at line no. %d in %s" %(lineNo, file))
                        
                        for item in resultList:
                            #print("Matched item: ", item)
                            if item in tableAliasList:
                                tableAliasList.remove(item)
            
            for item in tableAliasList:
                tableName= item.split(".")[0]
                feedback= "File: %s\n      Table %s at line no. %d has unused primary key!" %(file, tableName, tableLineNoIndicator[tableName])
                findingsList.append(feedback)
            
    saveFindings(outputFile, findingsList)

    print("END...")

#Call the main method to start processing.
tableDict= getTablesWithPrimaryKey(entityPath, primaryKeys).keys() 
tableList= list(tableDict)
print("No. of tables: ", len(tableList))
#print("Tables: ", tableList)
main(tableList, filesPath)