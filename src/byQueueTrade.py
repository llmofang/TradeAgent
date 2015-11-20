# -*- coding: cp936 -*-
from Queue import Queue
import threading
import time
from stockTradeClass import *
import datetime
import pandas as pd
import random

# CommandGetter�����ڽ���������䵽������

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
        # ��¼��һ�ε�����
        self.lastCampareList = []
        # ��ʱ��Ž��յ�������
        self.CacheQueue = []
        self.idleTimeBegin = idleTimeBegin
        self.combineCommandListDf = pd.DataFrame([])

    def run(self):
        while True:
            commandLineStr = self.readCommandData()
            if commandLineStr == '':
                # ���е�ʱ�򽫻�������뵽������еȴ�����

                # ����ؼ������޸��������
                mylock.acquire()

                if len(self.CacheQueue) > 0:
                    while len(self.CacheQueue) > 0:
                        self.data.put(self.CacheQueue.pop(0))
                        print 'self.data', self.data.qsize()

                # �ȽϿ���ʱ�䣬����ʱ�����3������гɽ�״̬���
                if (datetime.datetime.now() - self.idleTimeBegin[0]).seconds >= 3:
                    self.checkTradeResult()
                    self.idleTimeBegin[0] = datetime.datetime.now()

                # �뿪�ؼ��ؼ�����
                mylock.release()
            else:
                # ��������浽�������
                self.CacheQueue.append(commandLineStr)
                print 'get ', commandLineStr

    # readCommandData()���ڶ�ȡ����
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

    # checkTradeResult���ڼ��ɽ����
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

        # ����Ϊ�Ƚϵ�ǰ��ǰ����ʲô���𣬽�������Ĳ��ַ��ز����͵�ָ���ļ�

        # ������һ�α仯
        # self.lastCampareList=self.checkStock.combinedList
        self.sendResult(combindfdfeDf(newCombineCommandListDf, self.combineCommandListDf))
        self.combineCommandListDf = newCombineCommandListDf
        print datetime.datetime.now(), 'Check trade result in idle Time finished'

    # ���Ͳ�ѯ���Ŀ�������
    def sendResult(self, df):
        print df
        if df.shape[0] == 0:
            return
        now = datetime.datetime.now()
        filename = 'result' + now.strftime('%Y%m%d%H%M%S') + '.csv'
        print 'write to ' + filename
        df.to_csv(filename)


# CommandRunner������ִ�ж����е�����

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
            # ����ؼ������Ҽ��������б���
            # ȡ��һ��ָ��
            commandStr = self.data.get()
            # �����п�ʼʱ�丳һ���ܴ��ֵ����֤��ִ������ʱ�������ܵ�"�ɽ�״̬���"����ĸ���
            self.mylock.acquire()
            now = datetime.datetime.now()
            self.idleTimeBegin[0] = now + datetime.timedelta(hours=3600)
            self.mylock.release()
            # ��ÿ��������Ϻ�
            self.commandCount += 1
            # �������ں͵�Ȼ�µ���Ŀ�ϳ�������
            commandLineID = now.strftime('%Y%m%d') + '%05d' % (self.commandCount)
            commandLinestr = commandLineID + ' ' + commandStr
            # �������зֽ���б���ʽ
            commandLine = readCommandFromLinestr(commandLinestr)
            print 'running..... ', commandLine
            #time.sleep(2.5)
            print 'finish run ', commandLine

            # ��������ѡ����й�Ʊ�������ͳ�������
            if commandLine[1] == 'buy':
                # ���ù�Ʊ���롢�۸���Ŀ
                self.buyStock.changeParam(commandLine[2], commandLine[3], commandLine[4])
                self.buyStock.excuteList()
            if commandLine[1] == 'sell':
                self.sellStock.changeParam(commandLine[2], commandLine[3], commandLine[4])
                self.sellStock.excuteList()
            if commandLine[1] == 'cancel':
                # Ҫ�����µ������б�ֵ�������������
                self.cancelOrder.setCommandList(commandList)
                self.cancelOrder.excuteList(commandLine[2])

            # self.mylock.acquire()
            # shouldAddToCommandList = False
            # shouldAddToFailCommandList = False
            # # ����ǽ������������������ÿ�β�����ɺ���Ҫ���һ���Ƿ�ҵ��ɹ�.
            # # ������ɹ�Ҫ�Ѳ��ɹ��Ĳ�����¼����������ɹ�����ɹ��б�commandList
            # if commandLine[1] in ['#buy', '#sell']:  # test
            #     print 'check after buy and sell'
            #     self.checkStock.excuteList()
            #     # ����ҵ������͵ݽ�����������֮������һ����ʾ�ҵ��ǳɹ���
            #     if (len(self.checkStock.commissionList) == len(commandList) + 1):
            #         shouldAddToCommandList = True
            #     else:
            #         shouldAddToFailCommandList = True
            # shouldAddToCommandList = True  # test
            # if shouldAddToCommandList:
            #     commandList.append(commandLine)
            # if shouldAddToFailCommandList: self.failCommandList.append(commandLine)
            # # �������������ʵ�Ŀ��п�ʼʱ��
            # now = datetime.datetime.now()
            # self.idleTimeBegin[0] = now
            # self.mylock.release()


def main():
    # ������ʼʱ��
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
    commandList = []  # Shared resource ,���ڱ������й��Ĳ��ҳɹ�������
    main()
