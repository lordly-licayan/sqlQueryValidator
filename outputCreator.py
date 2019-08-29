import pandas as pd
import numpy as np
import xlsxwriter

from constants import *

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


def writeFindingsInPanda(findingsList, filesReadList, outputFile= "output.xlsx"):
    df = pd.DataFrame(findingsList, columns = ["Domain", "Screen ID", "Filename", "Line No", "Script Findings", "Result", "Remarks"])
    df1 = pd.DataFrame(filesReadList, columns = ["Filename checked:"])
    writer = pd.ExcelWriter(outputFile, engine='xlsxwriter')
    
    df.to_excel(writer, sheet_name="Findings", index=False, startrow=2, startcol =1)
    df1.to_excel(writer, sheet_name="Files checked", startrow=1)
    writer.save()


def writeFindings(findingsList, filesReadList, outputFile= "output.xlsx"):
    workbook = xlsxwriter.Workbook(outputFile)
    worksheet = workbook.add_worksheet("Findings")
    
    labelName = ["Domain", "Screen ID", "Filename", "Line No", "Script Findings", "Result", "Remarks"]
    colDomain, colScreenId, colFileName, colLineNo, colScriptFindings, colResult, colRemarks= 0, 1, 2, 3, 4, 5, 6
    rowDomain, rowScreenId, rowFileName, row, col= 1, 1, 1, 1, 0
    domainName, screenId, fileName = None, None, None
    totalFindings= len(findingsList)

    worksheet.write_string(0, colDomain, labelName[0])
    worksheet.write_string(0, colScreenId, labelName[1])
    worksheet.write_string(0, colFileName, labelName[2])
    worksheet.write_string(0, colLineNo, labelName[3])
    worksheet.write_string(0, colScriptFindings, labelName[4])
    worksheet.write_string(0, colResult, labelName[5])
    worksheet.write_string(0, colRemarks, labelName[6])

    for feedbackList in findingsList:
        worksheet.write_string(row, colDomain, feedbackList[0])
        worksheet.write_string(row, colScreenId, feedbackList[1])
        worksheet.write_string(row, colFileName, feedbackList[2])
        worksheet.write_string(row, colLineNo, feedbackList[3])
        worksheet.write_string(row, colScriptFindings, feedbackList[4])

        if row == 1:
            domainName= feedbackList[0]
            screenId= feedbackList[1]
            fileName= feedbackList[2]
        
        isEndOfList= row == totalFindings
        
        if domainName != feedbackList[0]:
            if (row - rowDomain) > 1:
                worksheet.merge_range(rowDomain, col, row-1, col, domainName)
            domainName= feedbackList[0]
            rowDomain= row
        
        if screenId != feedbackList[1]:
            if (row - rowScreenId) > 1:
                worksheet.merge_range(rowScreenId, colScreenId, row-1, colScreenId, screenId)
            screenId= feedbackList[1]
            rowScreenId= row

        if fileName != feedbackList[2]:
            if (row - rowFileName) > 1:
                worksheet.merge_range(rowFileName, colFileName, row-1, colFileName, fileName)
            fileName= feedbackList[2]
            rowFileName= row

        if isEndOfList:
            if rowDomain != row:
                worksheet.merge_range(rowDomain, col, row, col, domainName)
            if rowScreenId != row:
                worksheet.merge_range(rowScreenId, colScreenId, row, colScreenId, screenId)
            if rowFileName != row:
                worksheet.merge_range(rowFileName, colFileName, row, colFileName, fileName)
        
        row += 1

    worksheetFilesRead = workbook.add_worksheet("Files checked")
    worksheetFilesRead.write_string(0, 0, "List of files read:")
    row= 1
    for fileRead in filesReadList:
        worksheetFilesRead.write_string(row, 0, str(row))
        worksheetFilesRead.write_string(row, 1, fileRead)
        row += 1

    workbook.close()