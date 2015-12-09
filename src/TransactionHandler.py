# -*- coding: cp936 -*-
import threading
import numpy as np
import pandas as pd
from qpython.qtype import QException
from pandas import DataFrame
from Event import *


class TransactionHandler(threading.Thread):

    def __init__(self, q, event_queue, sym, logger):
        super(TransactionHandler, self).__init__()
        self._stop = threading.Event()
        self.q = q
        self.event_queue = event_queue
        self.sym = sym
        self.logger = logger

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def subscribe(self):
        self.logger.debug('subscribing transaction, sym = %s', self.sym)
        self.q.sync('.u.sub', np.string_('Transaction'), np.string_('' if self.sym == [] else self.sym))

    def get_data(self):
        transaction = pd.DataFrame()
        try:
            message = self.q.receive(data_only=False, raw=False, pandas=True)
            self.logger.debug('type: %s, message type: %s, data size: %s, is_compressed: %s ',
                              type(message), message.type, message.size, message.is_compressed)

            if isinstance(message.data, list):
                # unpack upd message
                if len(message.data) == 3 and message.data[0] == 'upd' and message.data[1] == 'Transaction':
                    if isinstance(message.data[2], DataFrame):
                        transaction = message.data[2]
                    else:
                        self.logger.error("message.data content error!")
            self.logger.debug('got new transaction data: transaction=%s', transaction.to_string())

        except QException, e:
                print(e)
        finally:
            return transaction

    def send_events(self, transaction):
        transaction = TransactionEvent(transaction)
        self.logger.debug('generating event TransactionEvent: type= %s,  event = %s', transaction.type, transaction)
        self.event_queue.put(transaction)

    def run(self):
        self.subscribe()
        while not self.stopped():
            transaction = self.get_data()
            if len(transaction > 0):
                self.send_events(transaction)
