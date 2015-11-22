import threading
from Queue import Queue
import numpy as np
import pandas as pd
from qpython.qtype import QException
from pandas import DataFrame
from Event import *


class ResponseHandler(threading.Thread):

    def __init__(self, q, events, response_table):
        super(ResponseHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.events = events
        self.response_table = response_table

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_all_orders(self):
        query = 'select from ' + self.response_table
        self.orders = self.q(query)

    def get_new_orders(self, event):
        new_orders = event.new_orders
        self.orders = pd.concat([self.orders, new_orders])
        # todo
        if self.q('set', np.string_('my_new_orders'), new_orders) =='my_new_orders' :
            print('set new orders to my_new_orders successful!')
        else:
            print('sset new orders to my_new_orders error!')

        if self.q('wsupd[`trade2; my_new_orders]') == 'trade2':
            print('wsupd trade2 successful! ')
        else:
            print('wsupd trade2  error!')

    def compute_changes(self, event):
        self.orders['changed'] = np.zeros(len(self.orders))
        raw_orders = event.orders
        raw_orders['tagged'] = np.zeros(len(raw_orders))

        new_orders = pd.merge

        # 更新已标记了委托号且未完成的委托
        # todo
        tagged = self.orders[self.orders.entrustno > 0]
        tagged_unfinished = tagged[tagged.askvol != tagged.bidvol]
        for i in range(len(tagged_unfinished)):
            changed = 0
            entrust_no = tagged_unfinished.loc[i]['entrustno']
            new_row = raw_orders[raw_orders[u'委托编号']==entrust_no]
            if tagged_unfinished.loc[i]['bidprice'] != new_row.loc[0][u'成交价格']:
                tagged_unfinished.loc[i]['bidprice'] = new_row.loc[0][u'成交价格']
                changed = 1
            if tagged_unfinished.loc[i]['bidvol'] != new_row.loc[0][u'成交数量']:
                tagged_unfinished.loc[i]['bidvol'] = new_row.loc[0][u'成交数量']
                changed = 1
            # todo update status
            # if tagged_unfinished.loc[i]['status'] != new_row.loc[0][u'委托状态']:
            #     tagged_unfinished.loc[i]['status'] = new_row.loc[0][u'委托状态']
            #     changed = 1
            tagged_unfinished.loc[i]['changed'] = changed

        # 标记委托号，并更新成交信息
        untagged = self.orders[self.orders.entrustno == 0]
        to_tagged = raw_orders[raw_orders[u'委托编号'] != self.orders[self.orders['entrustno'] > 0]['entrustno'] ]

        return 'changes'
        pass

    def send_changes(self, changes):
        # todo
        self.q('set', np.string_('my_changes'), changes)
        self.q('wsupd[`trade2; my_changes]')

    def run(self):
        self.get_all_orders()
        while True:
            try:
                event = self.events.get()
            except Queue.Empty:
                print('queue empty error!')
            else:
                if event is not None:
                    if event.type == 'OrderStatusEvent':
                        changes = self.compute_changes(event)
                        self.send_changes(changes)
                    elif event.type == "NewOrdersEvent":
                        self.get_new_orders(event)





