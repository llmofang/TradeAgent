# -*- coding: cp936 -*-
#
# ExcuteCommandBaseΪ���в�����Ļ����࣬��ʼ��ʱ����ִ��ָ���б��ļ�
# BuyAndSellStockΪ���������,���Էֱ��ò�ͬ��ָ���б��ļ���������
# CancelOrderΪ�������࣬��ʼ��ʱ����ָ������������б�CommandList��
#           �Լ��ڹ�Ʊ����в�ѯ�ı����ÿһ���иߵ���ֵStep
# CheckStockΪ��ѯ�ɽ�״̬���࣬��ѯ������������ԱcommissionList��

from dealShockOrder import *
from readClipboard import getTextFromClipboard
import pyautogui


class ExcuteCommandBase:
    # ��ֵ��ʱ��ָ��ָ���ļ���������ָ������
    def __init__(self, commandListFile):
        self.commandList = readCommands(commandListFile)

    # ִ��ָ��
    def excuteList(self):
        for line in self.commandList:
            if line[0] == 'moveto':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveTo(x, y)
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
                pyautogui.typewrite(line[2])
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


class BuyAndSellStock(ExcuteCommandBase):
    def __init__(self, commandListFile):
        ExcuteCommandBase.__init__(self, commandListFile)
        # ��ȡ��Ҫ�����Ĳ�����λ��
        self.ShockCodeId = getPointedIndex('stockcode', self.commandList)
        self.ShockPriceId = getPointedIndex('stockprice', self.commandList)
        self.ShockNumId = getPointedIndex('stocknum', self.commandList)

    # ��������
    def changeParam(self, stockcode, stockprice, stocknum):
        self.commandList[self.ShockCodeId][2] = stockcode
        self.commandList[self.ShockPriceId][2] = stockprice
        self.commandList[self.ShockNumId][2] = stocknum


class CancelOrder(ExcuteCommandBase):
    # stepΪ��ѯ���Ľ�����ÿһ����Ҫ�ƶ��Ĳ���
    def __init__(self, commandListFile, step):
        ExcuteCommandBase.__init__(self, commandListFile)
        self.cancelOrderStep = step

    # CancelIdΪҪȡ���ĵ���,CommandListΪ�´����Ҫ�����������б�stepΪÿһ����Ҫ�ƶ��Ĳ���
    def excuteList(self, cancelId):
        ExcuteCommandBase.excuteList(self)
        content = getTextFromClipboard()
        # ��ý��������еݽ��������б�
        commissionList = dealWithClipboard(content)
        # �������б�ͺ�̨���͵��б��Ӧ����
        combinedList = combineCommandAndCommission(self.commandList, commissionList, ['check', 'cancel'])
        # ���ݵ��Ų������б��е�λ��
        i = findPosition(cancelId, combinedList)
        print i
        # ���ƶ���Ӧ��λ�Ʋ�˫���������
        if i <> 999:
            pyautogui.moveRel(0, i * self.step)
            pyautogui.doubleClick()
            pyautogui.press('y')

            '''pyautogui.press('down',i)
            pyautogui.press('space')
            if combinedList[i][19]=='buy':
                pyautogui.press('x')
                pyautogui.press('y')
            if combinedList[i][19]=='sell':
                pyautogui.press('c')
                pyautogui.press('y')'''

    # ������excuteList֮ǰ��Ҫ�����µ�commandList��ֵ����
    def setCommandList(self, o_commandList):
        self.commandList = o_commandList


class CheckStock(ExcuteCommandBase):
    def excuteList(self):
        ExcuteCommandBase.excuteList(self)
        content = getTextFromClipboard()
        # ��¼��ѯ���
        self.commissionList = dealWithClipboard(content)
