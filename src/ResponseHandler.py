# -*- coding: utf-8 -*-
import multiprocessing
from Queue import Queue
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from qpython.qtype import QException
from pandas import DataFrame
from abc import ABCMeta, abstractmethod
import logging
import logging.config
import ConfigParser
from qpython import qconnection


class ResponseHandler(multiprocessing.Process):
    __metaclass__ = ABCMeta

    def __init__(self, events):
        super(ResponseHandler, self).__init__()
        cf = ConfigParser.ConfigParser()
        cf.read("tradeagent.conf")
        self.q_host = cf.get("db", "host")
        self.q_port = cf.getint("db", "port")

        self.events = events
        self.response_table = cf.get("db", "response_table")
        # self.status = {u'未报': 0, u'已报': 1, u'未成': 2, u'已报待撤': 3,  u'已成': 4,  u'已撤': 5,  u'废单': 6}
        self.kdb_var_prefix = cf.get("kdb", "var_prefix").split(',')
        pd.set_option('mode.chained_assignment', None)
        # pd.set_option('display.encoding', 'gbk')

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_all_orders(self):
        query = 'select from ' + self.response_table
        self.orders = self.q(query)
        print('get all orders: orders=%s', self.orders.to_string())
        # TODO 这样有问题吧？？？？
        # self.orders.reset_index()
        self.table_meta = self.orders.meta
        print('table meta= %s', self.table_meta)

    def get_new_orders(self, event):
        new_orders = event.new_orders
        print('got new orders: new_orders=%s', new_orders.to_string())

        print('new_orders.dtypes = %s', new_orders.dtypes)
        try:
            new_orders = new_orders[new_orders['status'] != 3]
            columns = ['entrustno', 'askvol', 'bidvol', 'withdraw', 'status']
            for column in columns:
                # why? somewhere changed from int32 to int64, so converting it back
                if new_orders.dtypes[column] != 'int32':
                    print('new orders column converting to int: column = %s', column)
                    new_orders[column] = new_orders[column].astype(int)

            if ('sym' not in new_orders.index.names) or ('qid' not in new_orders.index.names):
                print('set index [sym, qid] for new orders')
                new_orders = new_orders.set_index(['sym', 'qid'])

            if 'index' in new_orders.columns:
                print('drop index columns for new orders')
                new_orders = new_orders.drop(['index'], axis=1)
        except QException, e:
            print(e)

        new_orders.meta = self.table_meta
        # self.orders = pd.concat([self.orders, new_orders])
        print('before update new orders: orders=%s', self.orders.to_string())
        self.orders = pd.concat([self.orders, new_orders])
        print('after update new orders: orders=%s', self.orders.to_string())
        new_orders.meta = self.table_meta

        if event.update_kdb:
            # todo 撤单的无需更新
            try:
                kdb_var = self.kdb_var_prefix + '_news'
                if self.q('set', kdb_var, new_orders) == kdb_var:
                    print('set new orders to my_new_orders successful!')
                else:
                    print('set new orders to my_new_orders error!')

                if self.q('wsupd[`trade2; %s]' % kdb_var) == 'trade2':
                    print('wsupd trade2 successful! ')
                else:
                    print('wsupd trade2  error!')
            except QException, e:
                print(e)

    def compute_changes(self, event):
        try:
            self.orders['changed'] = np.zeros(len(self.orders))
            self.orders['tagged'] = np.zeros(len(self.orders))
            self.orders['tagged'] = self.orders['entrustno'].map(lambda x: 1 if x > 0 else 0)
            self.tagged_changes(event.orders)
            self.untagged_changes(event.orders, 2)
        except Exception, e:
            print('Exception: %s', e)

    # 更新已标记了委托号且未完成的委托
    @abstractmethod
    def tagged_changes(self, new_orders):
        raise NotImplementedError("Should implement tagged_changes()")

    # 标记委托号，并更新成交信息
    @abstractmethod
    def untagged_changes(self, new_orders, nearest_min):
        raise NotImplementedError("Should implement untagged_changes()")

    def send_changes(self, changes):
        print('changes length: %i', len(changes))
        print('changes dtypes: %s', changes.dtypes)
        if len(changes) > 0:
            # TODO
            # changes = changes.reset_index()
            # changes = changes.set_index(['sym', 'qid'])
            changes = changes.drop(['tagged', 'changed'], axis=1)
            print('changes dtypes=%s', changes.dtypes)
            print('send changes: changes=%s', changes.to_string())
            changes.meta = self.table_meta

            kdb_var = self.kdb_var_prefix + '_changes'

            if self.q('set', np.string_(kdb_var), changes) == kdb_var:
                print('set changes to my_changes successful!')
            else:
                print('set changes to my_changes error!')

            if self.q('wsupd[`trade2; %s]' % kdb_var) == 'trade2':
                print('wsupd my_changes to trade2 successful! ')
            else:
                print('wsupd my_changes to trade2  error!')

    def run(self):
        self.q = qconnection.QConnection(host=self.q_host, port=self.q_port, pandas=True)
        self.q.open()
        self.get_all_orders()
        while True:
            event = None
            try:
                while self.events.qsize() > 0:
                    event = self.events.get()
            except Queue.Empty:
                print('queue empty error!')
            else:
                if event is not None:
                    if event.type == 'OrderStatusEvent':
                        self.compute_changes(event)
                    elif event.type == "NewOrdersEvent":
                        self.get_new_orders(event)
