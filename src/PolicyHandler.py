# -*- coding: cp936 -*-
import threading
import numpy as np
import pandas as pd
from qpython.qtype import QException
from pandas import DataFrame
from Event import *
from qpython import qconnection
from MarketHandler import MarketHandler
from TransactionHandler import TransactionHandler
import logging
import logging.config
from Queue import Queue
from datetime import timedelta
import sys


class PolicyHandler(threading.Thread):
    def __init__(self, event_queue, syms, logger):
        super(PolicyHandler, self).__init__()
        self._stop = threading.Event()
        self.event_queue = event_queue
        # todo
        self.syms = syms
        self.logger = logger
        self.avgAskVol = 0
        self.avgBidVol = 0
        self.askSpeed = 0
        self.bidSpeed = 0
        self.speed = 0
        self.longest_time = timedelta(minutes=3)


    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def market_process(self, market):
        ask_vol_name = 'nAskVol'
        bid_vol_name = 'nBidVol'
        loc = len(market) - 1
        self.avgAskVol = 0
        self.avgBidVol = 0
        for i in range(1, 11):
            ask_vol_column = ask_vol_name + str(i)
            bid_vol_column = bid_vol_name + str(i)
            self.avgAskVol += market.iloc[loc][ask_vol_column] / 10 / 100
            self.avgBidVol += market.iloc[loc][bid_vol_column] / 10 / 100
        print('(avgAskVol, avgBidVol): (%i, %i)' % (self.avgAskVol, self.avgBidVol))

    def transaction_process(self, transaction):
        self.transactions = pd.concat([self.transactions, transaction])
        self.logger.debug(transaction.to_string())

    def run(self):
        while True:
            try:
                qsize = self.event_queue.qsize()
                if qsize > 3:
                    self.logger.error('event queue size is too big: qsize = %i', qsize)
                event = self.event_queue.get()
                if event is not None:
                    self.logger.debug('Got Event: event=%s', event)
                    if event.type == 'MarketEvent':
                        self.market_process(event.market)
                    elif event.type == 'TransactionEvent':
                        self.transaction_process(event.transaction)
            except Exception, e:
                self.logger.error(e)


if __name__ == '__main__':
    try:
        logging.config.fileConfig('log.conf')
        logger = logging.getLogger('main')
        queue = Queue()
        sym = '002010'
        q_m = qconnection.QConnection(host='183.136.130.82', port=5020, pandas=True)
        q_t = qconnection.QConnection(host='183.136.130.82', port=6011, pandas=True)
        q_m.open()
        q_t.open()

        market = MarketHandler(q_m, queue, sym, logger)
        transaction = TransactionHandler(q_t, queue, sym, logger)
        policy = PolicyHandler(queue, sym, logger)

        policy.start()
        market.start()
        transaction.start()

        sys.stdin.readline()

        policy.stop()
        transaction.stop()
        market.stop()

    except Exception, e:
        print(e)