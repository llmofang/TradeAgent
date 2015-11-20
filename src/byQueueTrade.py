# -*- coding: cp936 -*-
from Queue import Queue
import threading
import time
from stockTradeClass import *
import datetime
import pandas as pd
import random

# CommandGetter类用于接收命令语句到队列中

BUY_LIST_FILE = 'buyStockList.txt'
SELL_LIST_FILE = 'sellStockList.txt'
CHECK_LIST_FILE = 'checkStockList.txt'
CANCEL_LIST_FILE = 'cancelOrderList.txt'
CANCEL_ORDER_STEP = 16


class CommandGetter(threading.Thread):
    def __init__(self, t_name, queue, idleTimeBegin, checkListFilename):
        threading.Thread.__init__(self, name=t_name)
        self.data = queue
        self.test = 0
        self.checkStock = CheckStock(checkListFilename)
        # 记录上一次的内容
        self.lastCampareList = []
        # 临时存放接收到的命令
        self.CacheQueue = []
        self.idleTimeBegin = idleTimeBegin
        self.combineCommandListDf = pd.DataFrame([])

    def run(self):
        while True:
            commandLineStr = self.readCommandData()
            if commandLineStr == '':
                # 空闲的时候将缓存命令导入到命令队列等待调度

                # 进入关键区域，修改命令队列
                mylock.acquire()

                if len(self.CacheQueue) > 0:
                    while len(self.CacheQueue) > 0:
                        self.data.put(self.CacheQueue.pop(0))
                        print 'self.data', self.data.qsize()

                # 比较空闲时间，空闲时间大于3秒则进行成交状态检查
                if (datetime.datetime.now() - self.idleTimeBegin[0]).seconds >= 3:
                    self.checkTradeResult()
                    self.idleTimeBegin[0] = datetime.datetime.now()

                # 离开关键关键区域
                mylock.release()
            else:
                # 将新命令保存到缓存队列
                self.CacheQueue.append(commandLineStr)
                print 'get ', commandLineStr

    # readCommandData()用于读取命令
    def readCommandData(self):
        #if self.test < 10:
            #self.test += 1
            #if (self.test % 2) == 0:
                #return 'sell 600010 4.5 100'
            #else:
                #return 'buy 600010 3.7 100'
        #return ''


        # self.test = random.randint(0, 200)
        time.sleep(0.2)
        if not self.test in [6, 20]:
            # if self.test>180:self.test=0
            return ''
        else:
            if self.test % 3 == 0:
                return 'buy 600010 3.7 100'
            else:
                return 'sell 600010 4.5 200'
            print 'self.test', str(self.test)

    # checkTradeResult用于检查成交结果
    def checkTradeResult(self):
        print datetime.datetime.now(), 'Check trade result in idle Time begin...', self.idleTimeBegin[0]
        self.checkStock.excuteList()
        #commandListDf = changeCommandListToDataFrame(commandList)
        #print 'commandListDf'
        #print commandListDf
        commissionListDf = changeCommissionListToDataFrame(self.checkStock.commissionList)
        print 'commissionListDf'
        print commissionListDf
        newCombineCommandListDf = combineCommandListAndCommissionList(commandListDf, commissionListDf)
        print 'oldcombineCommandListDf'
        print self.combineCommandListDf
        print 'newCombineCommandListDf'
        print newCombineCommandListDf

        # 以下为比较当前和前面有什么区别，将有区别的部分返回并发送到指定文件

        # 跟新上一次变化
        # self.lastCampareList=self.checkStock.combinedList
        self.sendResult(combindfdfeDf(newCombineCommandListDf, self.combineCommandListDf))
        self.combineCommandListDf = newCombineCommandListDf
        print datetime.datetime.now(), 'Check trade result in idle Time finished'

    # 发送查询结果目标服务器
    def sendResult(self, df):
        print df
        if df.shape[0] == 0:
            return
        now = datetime.datetime.now()
        filename = 'result' + now.strftime('%Y%m%d%H%M%S') + '.csv'
        print 'write to ' + filename
        df.to_csv(filename)


# CommandRunner类用于执行队列中的命令

class CommandRunner(threading.Thread):
    def __init__(self, t_name, queue, idleTimeBegin, buyListFilename, sellListFilename, checkListFilename,
                 cancelListFilename, cancelOrderStep, mylock):
        threading.Thread.__init__(self, name=t_name)
        self.data = queue
        self.commandCount = 0
        self.buyStock = BuyAndSellStock(buyListFilename)
        self.sellStock = BuyAndSellStock(sellListFilename)
        self.checkStock = CheckStock(checkListFilename)
        self.cancelOrder = CancelOrder(cancelListFilename, cancelOrderStep)
        self.idleTimeBegin = idleTimeBegin
        self.failCommandList = []
        self.mylock = mylock

    def run(self):
        while True:
            # 进入关键区域并且加入命令列表中
            # 取出一条指令
            commandStr = self.data.get()
            # 给空闲开始时间赋一个很大的值，保证在执行命令时，不会受到"成交状态检查"命令的干扰
            self.mylock.acquire()
            now = datetime.datetime.now()
            self.idleTimeBegin[0] = now + datetime.timedelta(hours=3600)
            self.mylock.release()
            # 给每条命令编上号
            self.commandCount += 1
            # 根据日期和当然下单数目合成命令编号
            commandLineID = now.strftime('%Y%m%d') + '%05d' % (self.commandCount)
            commandLinestr = commandLineID + ' ' + commandStr
            # 将命令行分解成列表形式
            commandLine = readCommandFromLinestr(commandLinestr)
            print 'running..... ', commandLine
            #time.sleep(2.5)
            print 'finish run ', commandLine

            # 根据命令选择进行股票的买卖和撤单操作
            if commandLine[1] == 'buy':
                # 设置股票代码、价格、数目
                self.buyStock.changeParam(commandLine[2], commandLine[3], commandLine[4])
                self.buyStock.excuteList()
            if commandLine[1] == 'sell':
                self.sellStock.changeParam(commandLine[2], commandLine[3], commandLine[4])
                self.sellStock.excuteList()
            if commandLine[1] == 'cancel':
                # 要把最新的命令列表赋值给撤销命令对象
                self.cancelOrder.setCommandList(commandList)
                self.cancelOrder.excuteList(commandLine[2])

            # self.mylock.acquire()
            # shouldAddToCommandList = False
            # shouldAddToFailCommandList = False
            # # 如果是进行买入和卖出操作，每次操作完成后，需要检查一下是否挂单成功.
            # # 如果不成功要把不成功的操作记录下来，如果成功加入成功列表commandList
            # if commandLine[1] in ['#buy', '#sell']:  # test
            #     print 'check after buy and sell'
            #     self.checkStock.excuteList()
            #     # 如果挂单数量和递交的买卖申请之和数量一样表示挂单是成功的
            #     if (len(self.checkStock.commissionList) == len(commandList) + 1):
            #         shouldAddToCommandList = True
            #     else:
            #         shouldAddToFailCommandList = True
            # shouldAddToCommandList = True  # test
            # if shouldAddToCommandList:
            #     commandList.append(commandLine)
            # if shouldAddToFailCommandList: self.failCommandList.append(commandLine)
            # # 命令完成设置真实的空闲开始时间
            # now = datetime.datetime.now()
            # self.idleTimeBegin[0] = now
            # self.mylock.release()


def main():
    # 空闲起始时间
    idleTimeBegin = [datetime.datetime.now() + datetime.timedelta(hours=3600)]
    print idleTimeBegin
    queue = Queue()
    cg = CommandGetter('Pro.', queue, idleTimeBegin, CHECK_LIST_FILE)
    cr = CommandRunner('Con.', queue, idleTimeBegin, BUY_LIST_FILE, SELL_LIST_FILE, CHECK_LIST_FILE, CANCEL_LIST_FILE,
                       CANCEL_ORDER_STEP)
    # cg=CommandGetter('Pro.',queue,idleTimeBegin,'checkStockList.txt')
    # cr=CommandRunner('Con.',queue,idleTimeBegin,'buyStockList.txt','sellStockList.txt','checkStockList.txt','cancelOrderList.txt',16)
    cg.start()
    cr.start()


if __name__ == '__main__':
    import thread

    mylock = thread.allocate_lock()  # Allocate a lock
    commandList = []  # Shared resource ,用于保存运行过的并且成功的命令
    main()
