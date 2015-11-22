# -*- coding: cp936 -*-
import threading
import numpy as np
import pandas as pd
from qpython.qtype import QException
from pandas import DataFrame
from Event import *


class RequestHandler(threading.Thread):

    def __init__(self, q, events_response, events_trade, request_table, users):
        super(RequestHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.events_response = events_response
        self.events_trade = events_trade
        self.request_table = request_table
        if isinstance(users, list):
            self.users = users
        else:
            self.users = []

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def subscribe_request(self):
        # TODO
        self.q.sync('.u.sub', np.string_(self.request_table), np.string_('' if self.users == [] else self.users))

    def get_new_requests(self):
        index = ['time', 'sym', 'qid']
        columns = ['time', 'sym', 'qid', 'entrustno', 'stockcode', 'askprice', 'askvol', 'bidprice', 'bidvol', 'status']
        df_new_requests = pd.DataFrame([], index=index, columns=columns)
        try:
            message = self.q.receive(data_only=False, raw=False, pandas=True)
            print('type: %s, message type: %s, data size: %s, is_compressed: %s ' % (type(message), message.type, message.size, message.is_compressed))

            if isinstance(message.data, list):
                # unpack upd message
                if len(message.data) == 3 and message.data[0] == 'upd' and message.data[1] == self.request_table:
                    if isinstance(message.data[2], DataFrame):
                        df_new_requests = message.data[2]
                    elif isinstance(message.data[2], list):
                        df_new_requests = pd.concat(message.data[2], axis=1)
                        df_new_requests.columns = columns

                    else:
                        print("message.data content error!")
            print('df_new_requests:', df_new_requests)
            return df_new_requests

        except QException, e:
                print(e)

    def send_events(self, df_new_requests):
        new_order_event = NewOrdersEvent(df_new_requests)
        self.events_response.put(new_order_event)
        for key, row in df_new_requests.iterrows():
            # TODO symbol should be symbol type
            symbol = str(int(row.stockcode))
            price = str(row.askprice)

            direction = 'BUY' if int(row.askvol) > 0 else 'SELL'

            quantity = str(abs(int(row.askvol)))
            order_event = OrderEvent(symbol, direction, price, quantity)
            print('generate event: %s' % order_event)
            self.events_trade.put(order_event)
            print('RequestHandler events_out size: %s' % self.events_trade.qsize)

    def run(self):
        self.subscribe_request()

        while not self.stopped():
            print('.')
            df_new_requests = self.get_new_requests()
            self.send_events(df_new_requests)




