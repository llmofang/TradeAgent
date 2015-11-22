# -*- coding: cp936 -*-
import threading
from Queue import Queue
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from qpython.qtype import QException


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
        self.table_meta = self.orders.meta

    def get_new_orders(self, event):
        new_orders = event.new_orders
        new_orders[['qid','entrustno', 'stockcode', 'askvol', 'bidvol', 'status']] = \
            new_orders[['qid','entrustno', 'stockcode', 'askvol', 'bidvol', 'status']].astype(int)
        new_orders = new_orders.set_index(['time', 'sym', 'qid'])
        new_orders.meta = self.table_meta
        self.orders = pd.concat([self.orders, new_orders])
        # todo
        try:
            if self.q('set', np.string_('my_new_orders'), new_orders) == 'my_new_orders':
                print('set new orders to my_new_orders successful!')
            else:
                print('set new orders to my_new_orders error!')

            if self.q('wsupd[`trade2; my_new_orders]') == 'trade2':
                print('wsupd trade2 successful! ')
            else:
                print('wsupd trade2  error!')
        except QException, e:
                print(e)

    def compute_changes(self, event):
        try:
            self.orders['changed'] = np.zeros(len(self.orders))
            self.orders['tagged'] = np.zeros(len(self.orders))
            self.orders['tagged'] = self.orders['stockcode'].map(lambda x: True if x > 0 else False)
            new_orders = event.orders
            # new_orders['tagged'] = np.zeros(len(new_orders))

            # 更新已标记了委托号且未完成的委托
            # todo
            tagged = self.orders[self.orders.entrustno > 0]
            # todo cancel
            tagged_unfinished = tagged[tagged.askvol != tagged.bidvol]
            if len(tagged_unfinished) > 0:
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
            else:
                tagged_changes = []

            # 标记委托号，并更新成交信息
            nearest3m = datetime.now() - timedelta(minutes=3)
            untagged = self.orders[self.orders.entrustno < 1]
            untagged = untagged.reset_index()
            untagged = untagged[untagged['time'] > nearest3m]

            to_tagged = new_orders[new_orders[u'委托编号'].isin(self.orders['entrustno']) == False ]
            to_tagged = to_tagged.set_index([u'委托时间'])
            for i in range(len(untagged)):
                old = untagged['time'].iloc[i] - timedelta(minutes=2)
                new = untagged['time'].iloc[i] + timedelta(minutes=2)
                time_match = to_tagged.between_time(old, new)
                match = time_match[(time_match[u'证券代码'] == untagged['stockcode'].iloc[i]) & (time_match[u'委托价格'] == untagged['askprice'].iloc[i]) & \
                                   (time_match[u'委托数量'] == abs(untagged['askvol'].iloc[i])) & \
                                   (time_match[u'买卖'] == (u'买入' if untagged['askvol'].iloc[i] > 0 else u'卖出'))]
                if len(match) > 0:
                    if len(match) == 1:
                        print('Perfect match row!')
                    else:
                        print('Find more than 1 for row!')
                    untagged['entrustno'].iloc[i] = match[u'委托编号'].iloc[0]
                    untagged['bidprice'].iloc[i] = match[u'成交价格'].iloc[0]
                    untagged['bidvol'].iloc[i] = match[u'成交数量'].iloc[0]
                    untagged['changed'].iloc[i] = 1
                    # todo 撤单数量
                else:
                    print('Can not find matched order!')

            untagged = untagged.set_index(['time', 'sym', 'qid'])
            untagged_changes = untagged[untagged['changed'] == 1]

            changes = pd.concat([tagged_changes, untagged_changes])
            return changes
        except Exception, e:
            print(e)


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





