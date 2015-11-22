import threading
from Queue import Queue
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
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
        new_orders = event.orders
        #new_orders['tagged'] = np.zeros(len(new_orders))

        # 更新已标记了委托号且未完成的委托
        # todo
        tagged = self.orders[self.orders.entrustno > 0]
        # todo cancel
        tagged_unfinished = tagged[tagged.askvol != tagged.bidvol]
        for i in range(len(tagged_unfinished)):
            changed = 0
            entrust_no = tagged_unfinished.loc[i]['entrustno']
            new_row = new_orders[new_orders[u'委托编号']==entrust_no]
            if tagged_unfinished.loc[i]['bidprice'] != new_row.loc[0][u'成交价格']:
                tagged_unfinished.loc[i]['bidprice'] = new_row.loc[0][u'成交价格']
                changed = 1
            if tagged_unfinished.loc[i]['bidvol'] != new_row.loc[0][u'成交数量']:
                tagged_unfinished.loc[i]['bidvol'] = new_row.loc[0][u'成交数量']
                changed = 1
            # todo update status
            # if tagged_unfinished.loc[i]['status'] != new_row.loc[0][u'委托状态']:
            #     tagged_unfinished.loc[i]['status'] = new_row.loc[0][u'委托状态']
            tagged_unfinished.loc[i]['changed'] = changed
        tagged_changes = tagged_unfinished[tagged_unfinished[changed] == 1]

        # 标记委托号，并更新成交信息
        nearest30m = datetime.now() - timedelta(minutes=30)
        untagged = self.orders[self.orders.entrustno < 1]
        untagged = untagged.rest_index()
        untagged = untagged[untagged['time'] > nearest30m]

        to_tagged = new_orders[new_orders[u'委托编号'] != self.orders[self.orders['entrustno'] > 0]['entrustno'] ]
        to_tagged = to_tagged.set_index([u'委托时间'])
        for row in untagged.iterrows():
            old = row.time - timedelta(minutes=2)
            new = row.time + timedelta(minutes=2)
            time_match = to_tagged.between_time(old, new)
            match = time_match[(time_match[u'证券代码'] == row.stockcode) & (time_match[u'委托价格'] == row.askprice) & \
                               (time_match[u'委托数量'] == abs(row.askvol)) & \
                               (time_match[u'买卖'] == (u'买入' if row.askvol > 0 else u'卖出'))]
            if len(match) > 0:
                if len(match) ==1:
                    print('Perfect match row: %s' % row)
                else:
                    print('Find more than 1 for row: ' % row)
                row.entrustno = match[0][u'委托编号']
                row.bidprice = match[0][u'成交价格']
                row.bidvol = match[0][u'成交数量']
                row.changed = 1
                # todo 撤单数量
            else:
                print('Can not find row: %s' % row)

        untagged = untagged.set_index(['time', 'sym', 'qid'])
        untagged_changes = untagged[untagged['changed'] == 1]

        changes = pd.concat([tagged_changes, untagged_changes])
        return 'changes'

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





