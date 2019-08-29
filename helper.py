import os
import re

from constants import *

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
                if PHYSICAL_TABLE_NAME in line:
                    itemList= re.sub(r"[^\x00-\x7F]|\W+", SPACE_TAG, line).strip().upper().split(SPACE_TAG)
                    tableName= itemList[TABLE_NAME_INDEX]
                elif PRIMARY_KEY_INDICATOR in line:
                    pk= re.findall(primaryKeysPattern, line, re.IGNORECASE)
                    if pk:
                        pkList.append(pk[0])
            if tableName and pkList:
                result[tableName]= pkList
    return result


def getTableInfo(line):
    tableName= None
    tableAlias= None
    
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
