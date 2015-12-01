# -*- coding: cp936 -*-
import threading
import numpy as np
import pandas as pd
from qpython.qtype import QException
from pandas import DataFrame
from Event import *


class RequestHandler(threading.Thread):

    def __init__(self, q, events_response, events_trade, request_table, users, logger, event_types):
        super(RequestHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.events_response = events_response
        self.events_trade = events_trade
        self.request_table = request_table
        self.event_types = event_types
        self.logger = logger
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
        self.logger.debug('subscribe trade: table=%s, users=%s', self.request_table, self.users)
        self.q.sync('.u.sub', np.string_(self.request_table), np.string_('' if self.users == [] else self.users))

    def get_new_requests(self):
        index = ['sym', 'qid']
        columns = ['sym', 'qid', 'time', 'entrustno', 'stockcode', 'askprice', 'askvol', 'bidprice',
                   'bidvol', 'withdraw', 'status']
        df_new_requests = pd.DataFrame(columns=columns)
        try:
            message = self.q.receive(data_only=False, raw=False, pandas=True)
            self.logger.debug('type: %s, message type: %s, data size: %s, is_compressed: %s ',
                              type(message), message.type, message.size, message.is_compressed)

            if isinstance(message.data, list):
                # unpack upd message
                if len(message.data) == 3 and message.data[0] == 'upd' and message.data[1] == self.request_table:
                    if isinstance(message.data[2], DataFrame):
                        df_new_requests = message.data[2]
                    elif isinstance(message.data[2], list):
                        # TODO 有错误。。走不通
                        # df_new_requests = pd.concat(message.data[2], axis=1)
                        # df_new_requests.loc[0] = message.data[2]
                        # df_new_requests[['entrustno', 'stockcode', 'askvol', 'bidvol', 'withdraw', 'status']] = \
                        #     df_new_requests[['entrustno', 'stockcode', 'askvol', 'bidvol', 'withdraw', 'status']].astype(int)
                        # df_new_requests = df_new_requests.set_index(index)
                        self.logger.error('TODO: IT IS A LIST, I CAN NOT HANDLE IT NOW')
                    else:
                        self.logger.error("message.data content error!")
            self.logger.debug('new requests data: df_new_requests=%s', df_new_requests.to_string())

        except QException, e:
                print(e)
        finally:
            return df_new_requests

    def send_events(self, df_new_requests):
        if len(df_new_requests) > 0:
            update_kdb = True if 'OrderEvent' in self.event_types else False
            new_order_event = NewOrdersEvent(df_new_requests, update_kdb)
            self.logger.debug('generate NewOrdersEvent=', new_order_event)
            self.events_response.put(new_order_event)
            df_new_requests = df_new_requests.reset_index()
            event = []
            for key, row in df_new_requests.iterrows():
                if ('CancelOrderEvent' in self.event_types) and (row.status == 3):
                    event = CancelOrderEvent(str(row.qid), str(int(row.entrustno)))
                elif ('OrderEvent' in self.event_types) and (row.status != 3):
                    symbol = row.stockcode
                    price = str(round(row.askprice, 2))
                    direction = 'BUY' if int(row.askvol) > 0 else 'SELL'
                    if (row.askvol % 100) != 0:
                        self.logger.error('order quantity not multiple by 100: askvol=%i', row.askvol)
                        continue
                    quantity = str(abs(int(row.askvol)))
                    event = OrderEvent(symbol, direction, price, quantity)

                if event:
                    self.logger.debug('generate event: event=%s', event)
                    self.events_trade.put(event)
                    self.logger.debug('RequestHandler events_out size: %s', self.events_trade.qsize())

    def run(self):
        self.subscribe_request()
        while not self.stopped():
            df_new_requests = self.get_new_requests()
            self.send_events(df_new_requests)