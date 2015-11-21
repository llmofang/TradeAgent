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

    def get_unfinished_orders(self):
        query = 'select from ' + self.response_table
        all_orders = self.q.sync(query)
        return all_orders

    def get_new_orders(self, event):


    def compute_changes(self, event):
        pass

    def send_changes(self):
        pass

    def run(self):
        while True:
            try:
                event = self.events.get()
            except Queue.Empty:
                print('queue empty error!')
            else:
                if event is not None:
                    if event.type == 'OrderStatusEvent':
                        self.compute_changes(event)
                        self.send_changes()
                    elif event.type == "NewOrdersEvent":
                        self.get_new_orders(event)





