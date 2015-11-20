# -*- coding: cp936 -*-
import numpy
import threading
import sys

from qpython import qconnection
from qpython.qtype import QException
from qpython.qconnection import MessageType
from qpython.qcollection import QTable
import pandas as pd
from pandas import DataFrame, Series

from Queue import Queue
from stockTradeClass import *
from byQueueTrade import *
import datetime

class ListenerThread(threading.Thread):

    def __init__(self, q):
        super(ListenerThread, self).__init__()
        self.q = q
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        dfRequestTrades = self.q.sync('select from trade1')
        print('dfRequestTrades:', dfRequestTrades)
        dfResponseTrades = self.q.sync('select from trade2')
        print('dfResponseTrades:', dfResponseTrades)
        self.q.sync('.u.sub', numpy.string_('trade1'), numpy.string_(''))
        while not self.stopped():
            print('.')
            try:
                index = ['time', 'sym', 'qid']
                columns = ['time', 'sym', 'qid', 'entrustno', 'stockcode', 'askprice', 'askvol', 'bidprice', 'bidvol', 'status']
                dfNewRequstTrades = pd.DataFrame(data=[], index=index, columns= columns)
                message = self.q.receive(data_only=False, raw=False, pandas=True)  # retrieve entire message
                print('type: %s, message type: %s, data size: %s, is_compressed: %s ' % (type(message), message.type, message.size, message.is_compressed))

                if isinstance(message.data, list):
                    # unpack upd message
                    if len(message.data) == 3 and message.data[0] == 'upd' and message.data[1] == 'trade1':
                        if isinstance(message.data[2], DataFrame):
                            dfNewRequstTrades = dfNewRequstTrades = message.data[2].set_index(index)
                        elif isinstance(message.data[2], list):
                            dfNewRequstTrades = pd.concat(message.data[2], axis=1)
                            dfNewRequstTrades.columns = columns
                            dfNewRequstTrades = dfNewRequstTrades.set_index(index)
                        else:
                            print("message.data的内容和格式无法确定....")
                print('dfNewRequstTrades:', dfNewRequstTrades)


                # 请求处理

                dfRequestTrades = pd.concat([dfRequestTrades, dfNewRequstTrades])

                # generate command
                mylock.acquire()
                for key, row in dfNewRequstTrades.iterrows():
                    stockcode = row.stockcode
                    askprice = row.askprice
                    action = 'buy' if int(row.askvol) > 0 else 'sell'
                    askvol = abs(int(row.askvol))
                    command = action + ' ' + str(int(stockcode)) + ' ' + str(askprice) + ' ' + str(askvol)
                    print('generate command: %s' % command)
                    queue.put(command)
                mylock.release()


                    # add command to queue

                # add command to queue

                # get entrust list and parse it

                # send response to kdb

            except QException, e:
                print(e)


if __name__ == '__main__':
    import thread

    BUY_LIST_FILE = 'buyStockList.txt'
    SELL_LIST_FILE = 'sellStockList.txt'
    CHECK_LIST_FILE = 'checkStockList.txt'
    CANCEL_LIST_FILE = 'cancelOrderList.txt'

    mylock = thread.allocate_lock()  # Allocate a lock
    commandList = []  # Shared resource ,用于保存运行过的并且成功的命令

    idleTimeBegin = [datetime.datetime.now() + datetime.timedelta(hours=3600)]
    print idleTimeBegin
    queue = Queue()
    # cg = CommandGetter('Pro.', queue, idleTimeBegin, CHECK_LIST_FILE)
    cr = CommandRunner('Con.', queue, idleTimeBegin, BUY_LIST_FILE, SELL_LIST_FILE, CHECK_LIST_FILE, CANCEL_LIST_FILE,
                       CANCEL_ORDER_STEP, mylock)
    cr.start()
    with qconnection.QConnection(host='183.136.130.82', port=5030, pandas=True) as q:
        print('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))

        t = ListenerThread(q)

        t.start()

        sys.stdin.readline()

        t.stop()
