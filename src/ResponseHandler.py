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

    def __init__(self, q, events, response_table):
        super(ResponseHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.events = events
        self.response_table = response_table
        self.status = {u'δ��': 0, u'�ѱ�': 1, u'δ��': 2, u'δ��': 3,  u'�ѳ�': 4,  u'�ѳ�': 5,  u'�ϵ�': 6}

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_all_orders(self):
        query = 'select from ' + self.response_table
        self.orders = self.q(query)
        self.orders.reset_index()
        self.table_meta = self.orders.meta

    def get_new_orders(self, event):
        new_orders = event.new_orders
        # TODO: why? changed from int32 to int64
        print(new_orders.dtypes)
        new_orders[['entrustno', 'stockcode', 'askvol', 'bidvol', 'withdraw', 'status']] = \
            new_orders[['entrustno', 'stockcode', 'askvol', 'bidvol', 'withdraw', 'status']].astype(int)
        new_orders = new_orders.set_index(['sym', 'qid'])
        new_orders.meta = self.table_meta
        self.orders = pd.concat([self.orders, new_orders])
        new_orders.meta = self.table_meta
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
            self.orders['tagged'] = self.orders['entrustno'].map(lambda x: 1 if x > 0 else 0)
            changes = self.tagged_changes(event.orders)
            self.send_changes(changes)
            changes = self.untagged_changes(event.orders, 3)
            self.send_changes(changes)

        except Exception, e:
            print(e)

    # �����ѱ����ί�к���δ��ɵ�ί��
    def tagged_changes(self, new_orders):
        try:
            # δ��ɵ�ί�е�status<4
            tagged_unfinished = self.orders[(self.orders['tagged'] == 1) &
                                            (self.orders['status'] < 4)]
            for i in range(len(tagged_unfinished)):
                match = new_orders[new_orders[u'������'] == tagged_unfinished['entrustno'].iloc[i]]
                if len(match) > 0:
                    tagged_unfinished['bidprice'].iloc[i] = match[u'�ɽ��۸�'].iloc[0]
                    tagged_unfinished['bidvol'].iloc[i] = match[u'�ɽ�����'].iloc[0]
                    tagged_unfinished['withdraw'].iloc[i] = match[u'�ѳ�����'].iloc[0]
                    tagged_unfinished['status'].iloc[i] = self.status[match[u'ί��״̬'].iloc[0]]
                    tagged_unfinished['changed'].iloc[i] = 1
            changes = tagged_unfinished[tagged_unfinished['changed'] == 1]
            self.orders.update(changes)
            return changes
        except Exception, e:
            print(e)

    # ���ί�кţ������³ɽ���Ϣ
    def untagged_changes(self, new_orders, nearest_min):
        try:
            # ��orders�в��ҳ�������������ڣ�����ʱ��Ĳ���������û�б���ǵ�ί�м�¼
            nearest = datetime.now() - timedelta(minutes=nearest_min)
            untagged = self.orders[(self.orders['tagged'] == 0) & (self.orders['time'] > nearest)]

            # ��new_orders�в��ҳ�û��ƥ��ί�б�ŵļ�¼
            to_tag = new_orders[new_orders[u'������'].isin(self.orders['entrustno']) == False ]
            to_tag['tagged'] = np.zeros(len(to_tag))
            to_tag = to_tag.set_index([u'ί��ʱ��'])
            for i in range(len(untagged)):
                old = untagged['time'].iloc[i] - timedelta(minutes=2)
                new = untagged['time'].iloc[i] + timedelta(minutes=2)

                # ʱ����ƥ������2���ӵ�
                time_match = to_tag.between_time(old, new)
                match = time_match[(time_match[u'֤ȯ����'] == untagged['stockcode'].iloc[i]) &
                                   (time_match[u'ί�м۸�'] == untagged['askprice'].iloc[i]) &
                                   (time_match[u'ί������'] == abs(untagged['askvol'].iloc[i])) &
                                   (time_match[u'����'] == (u'����' if untagged['askvol'].iloc[i] > 0 else u'����')) &
                                   (time_match['tagged'] == 0)]
                if len(match) > 0:
                    if len(match) == 1:
                        print('Perfect match row!')
                    else:
                        print('Find more than 1 for row!')
                    untagged['entrustno'].iloc[i] = match[u'������'].iloc[0]
                    untagged['bidprice'].iloc[i] = match[u'�ɽ��۸�'].iloc[0]
                    untagged['bidvol'].iloc[i] = match[u'�ɽ�����'].iloc[0]
                    untagged['withdraw'].iloc[i] = match[u'�ѳ�����'].iloc[0]
                    untagged['status'].iloc[i] = self.status[match[u'ί��״̬'].iloc[0]]
                    untagged['changed'].iloc[i] = 1
                    untagged['tagged'].iloc[i] = 1

                else:
                    print('Can not find matched order!')
            changes = untagged[untagged['changed'] == 1]
            self.orders.update(changes)
            return changes
        except Exception, e:
            print(e)

    def send_changes(self, changes):
        # changes = changes.set_index(['sym', 'qid'])
        if len(changes) > 0:
            changes = changes.drop(['tagged', 'changed'], axis=1)
            print(changes.dtypes)
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