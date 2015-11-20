# -*- coding: cp936 -*-
import pyautogui
import os
import time
from readClipboard import *
import pandas as pd


# ���ݵ��Ų����б��е�λ��
def findPosition(cancelId, combinedList):
    i = 0
    while i < len(combinedList) and combinedList[i][18] <> cancelId:
        i += 1
    if i < len(combinedList):
        return i
    else:
        return 999


# ���б�����д���ļ�
def writeListToFile(theList, filename):
    try:
        fl = open(filename, 'w')
        for line in theList:
            linestr = ''
            for s in line:
                linestr = linestr + ',' + s
            linestr = linestr.lstrip(',')
            fl.write(linestr + '\n')
    finally:
        fl.close()


# ���ļ����ж�ȡ�����б�
def readCommands(filename):
    lines = []
    f = open(filename)
    try:
        fileline = f.readlines()
        for line in fileline:
            line = line.rstrip('\n')
            lines.append(list(line.split(' ')))
    finally:
        f.close()
    return lines


# �ֽ�����������
def readCommandFromLinestr(linestr):
    return list(linestr.split(' '))


# ���ļ��ж�ȡ��������
def readListFormFile(filename):
    lines = []
    f = open(filename)
    try:
        fileline = f.readlines()
        for line in fileline:
            line = line.rstrip('\n')
            lines.append(list(line.split(' ')))
    finally:
        f.close()
        # os.remove(filename)
    return lines


# ִ�������б�
def excuteList(lines):
    for line in lines:
        if line[0] == 'moveto':
            x = int(line[1])
            y = int(line[2])
            t = float(line[3])
            pyautogui.moveTo(x, y, t)
            print 'move to ', x, y

        if line[0] == 'doubleclick':
            t = float(line[1])
            pyautogui.doubleClick(interval=t)
            print 'doubleclick'

        if line[0] == 'press':
            key = line[1]
            presses = int(line[2])
            interval = float(line[3])
            pause = float(line[4])
            pyautogui.press(key, presses=presses, interval=interval, pause=pause)
            print 'press ', line[1], presses, interval, pause

        if line[0] == 'click':
            t = float(line[1])
            pyautogui.click(interval=t)
            print 'click'

        if line[0] == 'type':
            t = float(line[3])
            pyautogui.typewrite(line[2], t)
            print 'type ', line[2]

        if line[0] == 'hotkey':
            pyautogui.hotkey(line[1], line[2])
            print 'press ', line[1] + '+' + line[2]

        if line[0] == 'moverel':
            x = int(line[1])
            y = int(line[2])
            t = float(line[3])
            pyautogui.moveRel(x, y, t)
            print 'move Rel ', x, y


# ������а��в�ѯί�е����ݣ������չ涨��ʽ������ֵ
def dealWithClipboard(content):
    content = content.rstrip('\x00')
    content = content.split('\r\n')
    contents = []
    for c in content:
        c = c.rstrip('\t')
        contents.append(c.split('\t'))
    contents = contents[1:]
    return contents


# �������������ί�е����ݽ���ƥ��
def combineCommandAndCommission(commandList, CommissionList, excludeList):
    # excludeListΪ���ÿ��ǵ�����
    newList = []
    j = 0
    for i in range(len(commandList)):
        # ��Ҫ�����ѯ�����������Ϊ����ԭ�����֤ȯ����Բ���Ҳ����
        if (not (commandList[i][1] in excludeList)) and (commandList[i][2] == CommissionList[j][2]):
            list1 = [x for x in CommissionList[j]]
            list1.append(commandList[j][0])
            list1.append(commandList[j][1])
            newList.append(list1)
            print 'add', list1
        j += 1
    return newList


# ��ָ�������������б��д�ŵ�λ��
def getPointedIndex(pointedContent, lines):
    i = 0
    for line in lines:
        if line[1] == pointedContent:
            # print line
            return i
        i += 1
    return -1


# ��������ת��ΪDataFrame�ĸ�ʽ
def changeCommandListToDataFrame(commandList):
    newCommandList = [x for x in commandList if x[1] in ['#sell', '#buy']]
    if newCommandList == []:
        df = pd.DataFrame(columns=['index', 'commandOperation', 'commandStockCode', 'stockPrice', 'commandStockNumber'])
    else:
        df = pd.DataFrame(newCommandList,
                          columns=['index', 'commandOperation', 'commandStockCode', 'stockPrice', 'commandStockNumber'])
    return df


# ����ѯ����ת��ΪDataFrame�ĸ�ʽ
def changeCommissionListToDataFrame(commissionList):
    if commissionList == []:
        df = pd.DataFrame(columns=['time', 'stockCode', 'stockName', 'operation', 'status', \
                                   'askVol', 'dealVol', 'cancelVol', 'askPrice', 'dealPrice', \
                                   'contractNo', 'applyNo', 'entrustType', 'account', 'entrustNo', 'returnInfo'])
    else:
        df = pd.DataFrame(commissionList,
                          columns=['time', 'stockCode', 'stockName', 'operation', 'status', \
                                   'askVol', 'dealVol', 'cancelVol', 'askPrice', 'dealPrice', \
                                   'contractNo', 'applyNo', 'entrustType', 'account', 'entrustNo', 'returnInfo'])
    return df


# �ϲ������б�Ͳ�ѯ�����ݣ�����DataFrame��ʽ
def combineCommandListAndCommissionList(commandListDf, commissionListDf):
    return commandListDf.join(commissionListDf, how='inner')
    if commandListDf.shape[0] <> commissionListDf.shape[0]:
        # ���������һ�����ؿյ�DataFrame
        return pd.DataFrame(
            columns=['index', 'commandOperation', 'commandStockCode', 'stockPrice', 'commandStockNumber', 'commitTime', \
                     'applyTime', 'stockCode', 'stockName', 'operation', 'commitType', \
                     'commitState', 'commitPrice', 'commitNum', 'dealPrice', 'dealNum', 'cancelNum', \
                     'shareholderCode', 'shareCode', 'market', 'info', 'commitId'])
    else:
        # �����߽���ƴ��
        return commandListDf.join(commissionListDf, how='inner')


# �Ƚ�����combineListDf�����𣬰���������ݷ���
def combineDf(newcbf, oldcbf):
    newCount = newcbf.shape[0]
    oldCount = oldcbf.shape[0]
    print newCount, ' and ', oldCount
    changedDf = pd.DataFrame([])
    for i in range(oldCount):
        newc = list(newcbf.iloc[i].values)
        oldc = list(oldcbf.iloc[i].values)
        if newc <> oldc:
            changedDf = changedDf.append(newcbf.iloc[i], ignore_index=True)
    if newCount > oldCount:
        print newCount, '>', oldCount
        changedDf = changedDf.append(newcbf[oldCount:], ignore_index=True)
    return changedDf


if __name__ == '__main__':
    commandList = readListFormFile('commandList.txt')
    c1 = changeCommandListToDataFrame(commandList)
    print c1

    buyList = readCommands('buyStockList.txt')
    # ��ȡ����ʱ��Ҫ��ָ̬�����ݵļ���λ�õ����к�
    buyShockCodeId = getPointedIndex('stockcode', buyList)
    buyShockPriceId = getPointedIndex('stockprice', buyList)
    buyShockNumId = getPointedIndex('stocknum', buyList)

    # ��ȡ��γ��۹�Ʊ��ϵ������
    sellList = readCommands('sellStockList.txt')
    # ��ȡ����ʱ��Ҫ��ָ̬�����ݵļ���λ�õ����к�
    sellShockCodeId = getPointedIndex('stockcode', sellList)
    sellShockPriceId = getPointedIndex('stockprice', sellList)
    sellShockNumId = getPointedIndex('stocknum', sellList)

    # ��ȡ��γ���ί�е�����
    cancelList = readCommands('cancelOrderList.txt')
    # ��ȡ���ɽ�״��������
    checkList = readCommands('checkStockList.txt')

    # excuteList(checkList)
    content = getTextFromClipboard()
    commissionList = dealWithClipboard(content)
    print commissionList
    c2 = changeCommissionListToDataFrame(commissionList)
