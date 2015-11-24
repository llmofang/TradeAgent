# -*- coding: cp936 -*-
import threading
from Queue import Queue
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from qpython.qtype import QException
from pandas import DataFrame


class ResponseHandler(threading.Thread):

    def __init__(self, q, events, response_table, logger):
        super(ResponseHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.events = events
        self.response_table = response_table
        self.status = {u'未报': 0, u'已报': 1, u'未成': 2, u'已报待撤': 3,  u'已成': 4,  u'已撤': 5,  u'废单': 6}
        self.logger = logger
        pd.set_option('mode.chained_assignment', None)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_all_orders(self):
        query = 'select from ' + self.response_table
        self.orders = self.q(query)
        # TODO 这样有问题吧？？？？
        self.orders.reset_index()
        self.table_meta = self.orders.meta

    def get_new_orders(self, event):
        new_orders = event.new_orders
        self.logger.debug('got new orders: new_orders=%s', new_orders)
        # TODO: why? changed from int32 to int64
        # print(new_orders.dtypes)
        new_orders[['entrustno', 'stockcode', 'askvol', 'bidvol', 'withdraw', 'status']] = \
            new_orders[['entrustno', 'stockcode', 'askvol', 'bidvol', 'withdraw', 'status']].astype(int)
        new_orders = new_orders.set_index(['sym', 'qid'])
        new_orders.meta = self.table_meta
        # self.orders = pd.concat([self.orders, new_orders])
        self.logger.debug('before update new orders: orders=%s', self.orders)
        self.orders.update(new_orders)
        self.logger.debug('after update new orders: orders=%s', self.orders)
        new_orders.meta = self.table_meta
        # todo
        try:
            if self.q('set', np.string_('my_new_orders'), new_orders) == 'my_new_orders':
                self.logger.info('set new orders to my_new_orders successful!')
            else:
                self.logger.error('set new orders to my_new_orders error!')

            if self.q('wsupd[`trade2; my_new_orders]') == 'trade2':
                self.logger.info('wsupd trade2 successful! ')
            else:
                self.log.error('wsupd trade2  error!')
        except QException, e:
                print(e)

    def compute_changes(self, event):
        try:
            self.orders['changed'] = np.zeros(len(self.orders))
            self.orders['tagged'] = np.zeros(len(self.orders))
            self.orders['tagged'] = self.orders['entrustno'].map(lambda x: 1 if x > 0 else 0)
            changes = self.tagged_changes(event.orders)
            self.send_changes(changes)
            changes = self.untagged_changes(event.orders, 3)
            self.send_changes(changes)

        except Exception, e:
            print(e)

    # 更新已标记了委托号且未完成的委托
    def tagged_changes(self, new_orders):
        try:
            # 未完成的委托的status<4
            tagged_unfinished = self.orders[(self.orders['tagged'] == 1) &
                                            (self.orders['status'] < 4)]
            self.logger.debug('tagged and unfinished orders: tagged_unfinished=%s', tagged_unfinished)
            for i in range(len(tagged_unfinished)):
                match = new_orders[new_orders[u'申请编号'] == tagged_unfinished['entrustno'].iloc[i]]
                changed = 0
                if len(match) > 0:
                    if tagged_unfinished['bidprice'].iloc[i] != match[u'成交价格'].iloc[0]:
                        tagged_unfinished['bidprice'].iloc[i] = match[u'成交价格'].iloc[0]
                        changed = 1
                    ratio = 1 if tagged_unfinished['askvol'].iloc[i] > 0 else -1
                    if tagged_unfinished['bidvol'].iloc[i] != ratio * match[u'成交数量'].iloc[0]:
                        tagged_unfinished['bidvol'].iloc[i] = ratio * match[u'成交数量'].iloc[0]
                        changed = 1
                    if tagged_unfinished['withdraw'].iloc[i] != match[u'已撤数量'].iloc[0]:
                        tagged_unfinished['withdraw'].iloc[i] = match[u'已撤数量'].iloc[0]
                        changed = 1
                    if tagged_unfinished['status'].iloc[i] != self.status[match[u'委托状态'].iloc[0]]:
                        tagged_unfinished['status'].iloc[i] = self.status[match[u'委托状态'].iloc[0]]
                        changed = 1
                    tagged_unfinished['changed'].iloc[i] = changed
            changes = tagged_unfinished[tagged_unfinished['changed'] == 1]
            self.logger.debug('tagged and unfinished orders: changes=%s', changes)
            self.logger.debug('before update changes: orders=%s', self.orders)
            self.orders.update(changes)
            self.logger.debug('after update changes: orders=%s', self.orders)
            return changes
        except Exception, e:
            print(e)

    # 标记委托号，并更新成交信息
    def untagged_changes(self, new_orders, nearest_min):
        try:

            # 从orders中查找出最近三分钟以内（更早时间的不做处理），没有被标记的委托记录
            nearest = datetime.now() - timedelta(minutes=nearest_min)
            untagged = self.orders[(self.orders['tagged'] == 0) & (self.orders['time'] > nearest)]

            self.logger.debug('untagged orders: untagged=%s', untagged)
            # 从new_orders中查找出没有匹配委托编号的记录
            to_tag = new_orders[new_orders[u'申请编号'].isin(self.orders['entrustno']) == False ]
            to_tag['tagged'] = np.zeros(len(to_tag))
            to_tag = to_tag.set_index([u'委托时间'])
            for i in range(len(untagged)):
                old = untagged['time'].iloc[i] - timedelta(minutes=2)
                new = untagged['time'].iloc[i] + timedelta(minutes=2)

                # 时间上匹配上下2分钟的
                time_match = to_tag.between_time(old, new)
                match = time_match[(time_match[u'证券代码'] == untagged['stockcode'].iloc[i]) &
                                   (time_match[u'委托价格'] == untagged['askprice'].iloc[i]) &
                                   (time_match[u'委托数量'] == abs(untagged['askvol'].iloc[i])) &
                                   (time_match[u'买卖'] == (u'买入' if untagged['askvol'].iloc[i] > 0 else u'卖出')) &
                                   (time_match['tagged'] == 0)]
                self.logger.debug('find order match untagged order: match=%s', match)
                changed = 0
                if len(match) > 0:
                    if len(match) == 1:
                        self.logger.debug('Perfect match row!')
                    else:
                        self.logger.info('Find more than 1 for row!')
                    if untagged['entrustno'].iloc[i] != match[u'申请编号'].iloc[0]:
                        untagged['entrustno'].iloc[i] = match[u'申请编号'].iloc[0]
                        changed = 1
                    if untagged['bidprice'].iloc[i] != match[u'成交价格'].iloc[0]:
                        untagged['bidprice'].iloc[i] = match[u'成交价格'].iloc[0]
                        changed = 1
                    ratio = 1 if untagged['askvol'].iloc[i] > 0 else -1
                    if untagged['bidvol'].iloc[i] != ratio * match[u'成交数量'].iloc[0]:
                        untagged['bidvol'].iloc[i] = ratio * match[u'成交数量'].iloc[0]
                        changed = 1
                    if untagged['withdraw'].iloc[i] != match[u'已撤数量'].iloc[0]:
                        untagged['withdraw'].iloc[i] = match[u'已撤数量'].iloc[0]
                        changed = 1
                    if untagged['status'].iloc[i] != self.status[match[u'委托状态'].iloc[0]]:
                        untagged['status'].iloc[i] = self.status[match[u'委托状态'].iloc[0]]
                        changed = 1
                    untagged['tagged'].iloc[i] = 1
                    untagged['changed'].iloc[i] = changed

                else:
                    self.logger.error('Can not find matched order!')

            changes = untagged[untagged['changed'] == 1]
            self.logger.debug('untagged orders: changes=%s')
            self.logger.debug('before update changes: orders=%s', self.orders)
            self.orders.update(changes)
            self.logger.debug('after update changes: orders=%s', self.orders)

        except Exception, e:
            print(e)
        finally:
            return changes

    def send_changes(self, changes):
        # changes = changes.set_index(['sym', 'qid'])
        if len(changes) > 0:
            changes = changes.drop(['tagged', 'changed'], axis=1)
            # print(changes.dtypes)
            changes.meta = self.table_meta
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
                        self.compute_changes(event)
                    elif event.type == "NewOrdersEvent":
                        self.get_new_orders(event)