import threading
import numpy as np
import pandas as pd
from qpython.qtype import QException
from pandas import DataFrame
from Event import OrderEvent


class RequestHandler(threading.Thread):

    def __init__(self, q, events_in, events_out, request_table, response_table, users):
        super(RequestHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.events_in = events_in
        self.events_out = events_out
        self.request_table = request_table
        self.response_table = response_table
        if isinstance(users, list):
            self.users = users
        else:
            self.users = []

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_all_request(self):
        query = 'select from ' + self.request_table
        self.df_all_requests = self.q.sync(query)
        print('self.df_all_requests:', self.df_all_requests)

    def subscribe_request(self):
        # TODO
        self.q.sync('.u.sub', np.string_(self.request_table), np.string_('' if self.users == [] else self.users))
        # self.q.sync('.u.sub', np.string_(self.cancel_table), np.string_(self.users))

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
                        df_new_requests = message.data[2].set_index(index)
                    elif isinstance(message.data[2], list):

                        df_new_requests = pd.concat(message.data[2], axis=1)
                        df_new_requests.columns = columns
                        df_new_requests = df_new_requests.set_index(index)
                    else:
                        print("message.data content error!")
            print('df_new_requests:', df_new_requests)
            self.df_all_requests = pd.concat([self.df_all_requests, df_new_requests])
            return df_new_requests

        except QException, e:
                print(e)

    def parse_new_request(self, df_new_requests):
        for key, row in df_new_requests.iterrows():
            # TODO symbol should be symbol type
            symbol = str(int(row.stockcode))
            price = str(row.askprice)

            direction = 'BUY' if int(row.askvol) > 0 else 'SELL'

            quantity = str(abs(int(row.askvol)))
            event = OrderEvent(symbol, direction, price, quantity)
            print('generate event: %s' % event)
            self.events_out.put(event)
            print('RequestHandler events_out size: %s' % self.events_out.qsize)

    def send_response(self, df):
        pass

    def get_order_status(self):
        pass

    def update_order_status(self):
        pass

    def run(self):
        self.get_all_request()
        self.subscribe_request()

        while not self.stopped():
            print('.')
            df_new_requests = self.get_new_requests()
            self.parse_new_request(df_new_requests)





