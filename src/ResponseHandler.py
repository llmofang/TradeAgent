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
        self.all_orders = self.q(query)

    def get_new_orders(self, event):
        new_orders = event.new_orders
        self.all_orders = pd.concat([self.all_orders, new_orders])
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
        # convert to event's new_order to df, order by time
        # del finished
        #
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





