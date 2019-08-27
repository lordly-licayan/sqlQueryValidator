import pandas as pd
import numpy as np
import os
import re
from datetime import datetime 


TABLE_TAG= "_TBL"
TAB_TAG= "\t+"
DOT_TAG= "."
FULL_WIDTH_DOT_TAG= "．"
SPACE_TAG= " "
EMPTY_TAG= ""
AS_TAG= "AS"
ON_TAG= "ON"
UNION_TAG= "UNION"
FROM_TAG= "FROM"
JOIN_TAG= "JOIN"
WHERE_TAG= "WHERE"
WHERE_END_TAG= "</WHERE>"
EXISTS_TAG= "EXISTS"
EQUAL_TAG= "="
EQUAL_SPACE_TAG= ' = '
PARAM_TAG= "#{"
OPEN_PARENTHESIS_TAG= "("
SELECTOR_BEGIN_TAG= "<"
SELECTOR_END_TAG= ">"
SQL_QUERY_START_TAG= "<SELECT ID"
SQL_QUERY_END_TAG= "</SELECT>"

INVALID_SQL= "Invalid SQL statement"
BROKEN_SQL= "Broken JOIN statement!"
PARAM_VALUE_USED= "Parameter value %s is used. This should be placed in the WHERE clause."
LONG_SQL_LINE= "\nLonger JOIN statement encountered!"
MUST_HAVE_PK= "%s.%s and %s.%s should be used in the JOIN instead of %s and %s."
NOT_IN_WHERE_CLAUSE= "Expected %s is not present in the WHERE clause."

NEWLINE= "\n"
TABLE_NAME_INDEX= 2
DOMAIN_NAME= "domain"
JOB_NAME= "job"
DOMAIN_NAME_INDEX= 1
SCREEN_ID_INDEX= 2

SQL_JOIN_PARTNERS= ["OUTER","INNER","LEFT"] 
SQL_KEYWORDS=["SELECT", "FROM", "LEFT", "RIGHT", "INNER", "OUTER", "JOIN", "GROUP", "ORDER", "BY", "EXISTS", "UNION"]
SQL_CONNECTOR_TAGS= ["AND","OR"]

primaryKeys= ["KAISHA_CD"]
primaryKeyIndicator= "primaryKeyMap.put"
physicalTableName= "DatabaseTableInfo"
resultTypeIndicator= "resultType"

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
    
    #if DOT_TAG not in line and TABLE_TAG in line:
    if TABLE_TAG in line:
        newLine= re.sub(r"[^\x00-\x7F]|[^\w+|^\.]", SPACE_TAG, line).strip()
        index= re.search(r"\w+_TBL", newLine)
        
        if index:
            splitResult= newLine[index.start():].split(SPACE_TAG)
            tableName= splitResult[0]
            
            if len(splitResult)>= 2:
                if splitResult[1] in SQL_KEYWORDS:
                    tableAlias= tableName
                elif splitResult[1] == AS_TAG and len(splitResult) >= 3:
                    tableAlias= splitResult[2]
                else:
                    tableAlias= splitResult[1]
            else:
                tableAlias= tableName

    """ if tableName:
        print("tableName is %s, alias is %s" %(tableName, tableAlias)) """
    return tableName, tableAlias


def writeFindings(findingsList, filesReadList, outputFile= "output.xlsx"):
    df = pd.DataFrame(findingsList, columns = ["Domain", "Screen ID", "Filename", "Line No", "Script Findings", "Result", "Remarks"])
    df1 = pd.DataFrame(filesReadList, columns = ["Filename checked:"])
    writer = pd.ExcelWriter(outputFile, engine='xlsxwriter')
    df.to_excel(writer, sheet_name="Findings", index=False, startrow=2, startcol =1)
    df1.to_excel(writer, sheet_name="Files checked", startrow=1)
    writer.save()


def outputFindings(fileName, findingsList, filesReadList, noOfFilesAffected=0):
    with open(fileName, 'w', encoding='utf-8') as fp:
        print("Writing feedback...")
        feedback= "No errors found!"
        feedbackList= []

        if findingsList:
            print(">>> Error(s) found!")
            for items in findingsList:
                feedbackList.append(",".join(items))
            
            feedback= ">>No. of files affected: %d\nNo. of issues detected: %d\nList of possible error(s):\n%s"  %(noOfFilesAffected, len(feedbackList), NEWLINE.join(feedbackList))
        else:
            print(">>> No errors found")
        
        fp.write(feedback)
        fp.write("\n\nList of files read (%d):\n%s" %(len(filesReadList),NEWLINE.join(filesReadList)))


def logFindings(findingsList, domainName, screenIdName, actualFileName, lineNo, remarks):
    feedbackList= []
    feedbackList.append(domainName)
    feedbackList.append(screenIdName)
    feedbackList.append(actualFileName)
    feedbackList.append(str(lineNo))
    feedbackList.append(remarks)
    feedbackList.append(SPACE_TAG)
    feedbackList.append(SPACE_TAG)
    
    findingsList.append(feedbackList)
    return feedbackList


def collectTableInfo(line, primaryKeys, physicalTableList, tableList, tableAliasList, tableAliasListWithPk, patternList):
    tableName, tableAlias= getTableInfo(line)
    #Collect table information and aliases
    if tableName is not None and tableName in tableList:
        #print("tableName is %s, alias is %s" %(tableName, tableAlias))
        for pk in primaryKeys:
            tableAliasList.append(tableAlias)
            tableAliasWithPk= "%s.%s" %(tableAlias,pk)

            if tableAliasWithPk not in tableAliasListWithPk:
                physicalTableList.append(tableName)
                tableAliasListWithPk[tableAliasWithPk]= 0
                patternList.append(r"\b%s\b" %tableAliasWithPk)
                    

def main(tableList, searchPath):
    xmlFiles, fileNames= getFiles(searchPath)
    filesReadList= []
    findingsList= []
    noOfFilesAffected= 0
    regexOnTag= re.compile(r"\b%s\b" %ON_TAG)
    regexOnNotWhere= re.compile(r"\b" + r"\b|\b".join(SQL_KEYWORDS) + r"\b")
    regexOnSqlJoin= re.compile("|".join(SQL_JOIN_PARTNERS))
    regexCleanLine= re.compile(r"(<!--(.*)-->)|(/\*(.*)\*/)|(--(.*))| +|\t+")
    regexFilename= re.compile(r"((\w*)S\d\d)|((\w*)J\d\d)")
    regexXmlFile= re.compile(r"(\w*).xml")

    for file in xmlFiles:
        domainName= None
        screenIdName= None
        actualFileName= None
        indexDomain= re.search("%s|%s" %(DOMAIN_NAME,JOB_NAME), file)

        if indexDomain:
            fileInfo= file[indexDomain.start():].split("\\")
            domainName= fileInfo[DOMAIN_NAME_INDEX]          
            screenIdSearch= regexFilename.search(regexXmlFile.search(file).group())
            if screenIdSearch:
                screenIdName= screenIdSearch.group()
            else:
                screenIdName= fileInfo[SCREEN_ID_INDEX]
            actualFileName= os.path.basename(file)
        else:
            continue

        with open(file, 'rt', encoding='utf-8') as fp:
            print("File: %s" %file)
            filesReadList.append(file)
            lineNo= 0
            patternList=[]
            physicalTableList=[]
            tableAliasList=[]
            tableAliasListWithPk={}
            tableJoinIsNextLine= False
            tableInBrokenJoin= None
            isInWhereClause= False
            countWhereClause= 0
            whereLineNo= 0

            for line in fp:
                lineNo+= 1
                if resultTypeIndicator in line:
                    continue

                line= re.sub(TAB_TAG, EMPTY_TAG, line)
                line= re.sub(FULL_WIDTH_DOT_TAG, DOT_TAG, line)
                line= re.sub(EQUAL_TAG, EQUAL_SPACE_TAG, line)
                line=  regexCleanLine.sub(SPACE_TAG, line).upper().strip()
                
                if not line or line.startswith(SQL_QUERY_START_TAG):
                    continue
                
                if regexOnNotWhere.search(line):
                    isInWhereClause= False
                elif WHERE_END_TAG == line:
                    continue
                elif WHERE_TAG in line:
                    isInWhereClause= True
                    countWhereClause += 1
                    whereLineNo= lineNo

                newLines= list(filter(None, regexOnSqlJoin.sub(EMPTY_TAG, line).strip().split(JOIN_TAG)))
                for item in newLines:
                    collectTableInfo(item, primaryKeys, physicalTableList, tableList, tableAliasList, tableAliasListWithPk, patternList)
                #print("physicalTableList: ", physicalTableList)
                #print("patternList: ", patternList)
                #print("tableAliasListWithPk: ", tableAliasListWithPk)
                
                """
                Check if the line contains ON tag.
                Scenarios on how 'ON' is used in a line:
                    1. within the join statement of a line  ex: INNER JOIN B ON A.COLUMN = B.COLUMN
                    2. starting word of the line            ex: ON A.COLUMN = B.COLUMN
                    3. last word of the line                ex: INNER JOIN B ON
                    4. the only word of the line            ex: ON
                """
                remarks= ""
                onTagSearch= regexOnTag.search(line)
                if onTagSearch or tableJoinIsNextLine:
                    if not tableJoinIsNextLine:
                        line = line[onTagSearch.end():]
                        
                    if not line:
                        tableJoinIsNextLine= True
                        continue

                    tableJoin = line.strip().split(SPACE_TAG)
                    
                    #1. within the join statement of a line and 2. starting word of the line
                    #if both tables have a common primary keys, then it must be used on the Join clause.
                    #Parameter value passed must not be used in join.
                    
                    if len(tableJoin) >= 2 and not (EQUAL_TAG == tableJoin[1]):
                        remarks= INVALID_SQL
                    elif len(tableJoin) == 1:
                        if OPEN_PARENTHESIS_TAG == tableJoin[0]:
                            tableJoinIsNextLine= False
                            continue
                        remarks= BROKEN_SQL
                    elif len(tableJoin) == 2:
                        tableInBrokenJoin= tableJoin[0].strip()
                        if PARAM_TAG in tableInBrokenJoin:
                            remarks= PARAM_VALUE_USED %tableInBrokenJoin
                        else:
                            tableJoinIsNextLine= False
                            continue
                    else:
                        if PARAM_TAG in tableJoin[0]:
                            remarks= PARAM_VALUE_USED %tableJoin[0]
                        elif PARAM_TAG in tableJoin[2]:
                            remarks= PARAM_VALUE_USED %tableJoin[2]
                        else:
                            firstGroup= tableJoin[0].strip()
                            firstTable= firstGroup.split(DOT_TAG)[0]

                            secondGroup= tableJoin[2].strip()
                            secondTable= secondGroup.split(DOT_TAG)[0]

                            if firstTable in tableAliasList and secondTable in tableAliasList:
                                if not (firstGroup in tableAliasListWithPk and secondGroup in tableAliasListWithPk):
                                    remarks= MUST_HAVE_PK %(firstTable,primaryKeys[0],secondTable,primaryKeys[0],firstGroup,secondGroup)
                        
                        if len(tableJoin) >= 4 and not tableJoin[3] in SQL_CONNECTOR_TAGS:
                            remarks += LONG_SQL_LINE

                elif DOT_TAG in line:
                    if tableInBrokenJoin:
                        firstGroup= tableInBrokenJoin.strip()
                        firstTable= firstGroup.split(DOT_TAG)[0]

                        secondGroup= line.split(SPACE_TAG)[0]
                        secondTable= secondGroup.split(DOT_TAG)[0]

                        if secondTable:
                            if PARAM_TAG in line:
                                remarks= PARAM_VALUE_USED %tableJoin[2]
                            elif firstTable in tableAliasList and secondTable in tableAliasList:
                                if not (tableInBrokenJoin in tableAliasListWithPk and secondGroup in tableAliasListWithPk):
                                    remarks= MUST_HAVE_PK %(firstTable,primaryKeys[0],secondTable,primaryKeys[0],firstGroup,secondGroup)
                        else:
                            remarks= BROKEN_SQL
                        
                        tableInBrokenJoin= None
                        continue

                    #Where Clause checking!
                    if isInWhereClause:
                        #print(patternList)
                        #print("Line no. %d: %s" %(lineNo, line))
                        if patternList:
                            #Just in case tweak is needed.
                            #searchPatten= '|'.join(patternList)
                            #regex= re.compile(searchPatten, re.IGNORECASE)
                            #resultList = regex.findall(line)
                            
                            #Assuming that the first table found after FROM has kaishaCd as primary key
                            resultList = re.search(patternList[0], line)
                            if resultList:
                                #print("Line no. %d: %s" %(lineNo, line))
                                #print("tableAliasListWithPk: ", tableAliasListWithPk)
                                tableAliasListWithPk[resultList.group()] += 1

                elif tableInBrokenJoin:
                    if PARAM_TAG in line:
                        remarks= PARAM_VALUE_USED %line
                    else:
                        remarks= BROKEN_SQL
                
                tableJoinIsNextLine= False
                tableInBrokenJoin= None

                if remarks:
                    noOfFilesAffected += 1
                    logFindings(findingsList, domainName, screenIdName, actualFileName, lineNo, remarks)

                #New SQL query detected, time to reinitialize variables used.
                elif line.startswith(SQL_QUERY_END_TAG):
                    if countWhereClause > 0 and tableAliasListWithPk:
                        if list(tableAliasListWithPk.values())[0] == 0:
                            remarks= NOT_IN_WHERE_CLAUSE %list(tableAliasListWithPk.keys())[0]
                            logFindings(findingsList, domainName, screenIdName, actualFileName, whereLineNo, remarks)
                    
                    patternList=[]
                    physicalTableList=[]
                    tableAliasList=[]
                    tableAliasListWithPk={}
                    tableJoinIsNextLine= False
                    tableInBrokenJoin= None
                    isInWhereClause= False
                    countWhereClause= 0    

    outputFindings(outputFile, findingsList, filesReadList, noOfFilesAffected)
    writeFindings(findingsList, filesReadList, outputFileXls)

#Call the main method to start processing.
start = datetime.now() 

print("STARTING... %s" %start)
tableDict= getTablesWithPrimaryKey(pathEntity, primaryKeys).keys() 
tableList= list(tableDict)
print("No. of tables: ", len(tableList))
#print("Tables: ", tableList)

main(tableList, pathWeb)
finish = datetime.now()
print("END... %s" %finish)
print('Scan Duration: %s'%(finish-start)) 