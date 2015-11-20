# -*- coding: cp936 -*-
#
# ExcuteCommandBase为所有操作类的基础类，初始化时必须执行指令列表文件
# BuyAndSellStock为买和卖的类,可以分别用不同的指令列表文件加以区分
# CancelOrder为撤单的类，初始化时还需指定命令（买卖）列表CommandList，
#           以及在股票软件中查询的表格里每一个行高的数值Step
# CheckStock为查询成交状态的类，查询结果保存在类成员commissionList中

from dealShockOrder import *
from readClipboard import getTextFromClipboard
import pyautogui


class ExcuteCommandBase:
    # 初值化时，指定指令文件，并导入指令序列
    def __init__(self, commandListFile):
        self.commandList = readCommands(commandListFile)

    # 执行指令
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
        # 获取需要调整的参数的位置
        self.ShockCodeId = getPointedIndex('stockcode', self.commandList)
        self.ShockPriceId = getPointedIndex('stockprice', self.commandList)
        self.ShockNumId = getPointedIndex('stocknum', self.commandList)

    # 调整参数
    def changeParam(self, stockcode, stockprice, stocknum):
        self.commandList[self.ShockCodeId][2] = stockcode
        self.commandList[self.ShockPriceId][2] = stockprice
        self.commandList[self.ShockNumId][2] = stocknum


class CancelOrder(ExcuteCommandBase):
    # step为查询表格的界面中每一条需要移动的步长
    def __init__(self, commandListFile, step):
        ExcuteCommandBase.__init__(self, commandListFile)
        self.cancelOrderStep = step

    # CancelId为要取消的单号,CommandList为下达过的要操作的命令列表，step为每一条需要移动的步长
    def excuteList(self, cancelId):
        ExcuteCommandBase.excuteList(self)
        content = getTextFromClipboard()
        # 获得界面中所有递交的任务列表
        commissionList = dealWithClipboard(content)
        # 将任务列表和后台递送的列表对应起来
        combinedList = combineCommandAndCommission(self.commandList, commissionList, ['check', 'cancel'])
        # 根据单号查找在列表中的位置
        i = findPosition(cancelId, combinedList)
        print i
        # 向移动相应的位移并双击鼠标卖出
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

    # 在运行excuteList之前需要把最新的commandList赋值过来
    def setCommandList(self, o_commandList):
        self.commandList = o_commandList


class CheckStock(ExcuteCommandBase):
    def excuteList(self):
        ExcuteCommandBase.excuteList(self)
        content = getTextFromClipboard()
        # 记录查询结果
        self.commissionList = dealWithClipboard(content)
