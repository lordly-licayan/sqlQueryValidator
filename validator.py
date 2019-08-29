import re

from helper import *
from outputCreator import *
from constants import *


def doValidate(tableList, searchPath, primaryKeys= [], outputFileLog="log.txt", outputFileXls="output.xlsx"):
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
                if RESULT_TYPE_INDICATOR in line:
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

    outputFindings(outputFileLog, findingsList, filesReadList, noOfFilesAffected)
    writeFindings(findingsList, filesReadList, outputFileXls)