# -*- coding: utf-8 -*-
import pyautogui
import threading
import Queue
from datetime import datetime
from datetime import timedelta
from Event import OrderStatusEvent
import pandas as pd
from winguiauto import *


class W32TradeHandler(threading.Thread):
    def __init__(self, hwnd_title, buy_hwnds_index, sell_hwnds_index, rz_buy_hwnds_index, rz_sell_hwnds_index,
                 rz_stocks, events_in, logger):
        super(W32TradeHandler, self).__init__()
        self._stop = threading.Event()

        self.hwnd_title = hwnd_title
        self.buy_hwnds_index = buy_hwnds_index
        self.sell_hwnds_index = sell_hwnds_index

        self.rz_buy_hwnds_index = rz_buy_hwnds_index
        self.rz_sell_hwnds_index = rz_sell_hwnds_index

        self.rz_stocks = rz_stocks

        self.events_in = events_in
        self.logger = logger

        self.last_check_orders_time = datetime.now()

        # 获取交易软件句柄
        self.hwnd_parent = findSpecifiedTopWindow(wantedText=self.hwnd_title)
        if self.hwnd_parent == 0:
            # TODO
            self.logger.error(u'错误, 请先打开交易软件，再运行本软件')
            return

        hwnd_child1 = dumpSpecifiedWindow(self.hwnd_parent, wantedClass='AfxMDIFrame42s')
        hwnd_child2 = dumpSpecifiedWindow(hwnd_child1[self.buy_hwnds_index[0][0]])
        self.buy_control_hwnds = dumpSpecifiedWindow(hwnd_child2[self.buy_hwnds_index[0][1]])
        hwnd_child1 = dumpSpecifiedWindow(self.hwnd_parent, wantedClass='AfxMDIFrame42s')
        hwnd_child2 = dumpSpecifiedWindow(hwnd_child1[self.sell_hwnds_index[0][0]])
        self.sell_control_hwnds = dumpSpecifiedWindow(hwnd_child2[self.sell_hwnds_index[0][1]])

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def close_popup_window(self, wantedText=None, wantedClass=None):
        # 如果有弹出式窗口，点击它的确定按钮
        hwnd_popup = findPopupWindow(self.hwnd_parent)
        if hwnd_popup:
            hwnd_control = findControl(hwnd_popup, wantedText, wantedClass)
            clickButton(hwnd_control)
            time.sleep(1)
            return True
        return False

    def order(self, event):
        if self.close_popup_window(self.hwnd_parent, wantedClass='Button'):
            print '^O^'
            time.sleep(0.3)
        time.sleep(0.1)
        if event.direction == 'BUY':
            hwnd_controls = self.buy_control_hwnds
            hwnd_index = self.buy_hwnds_index[1]
        elif event.direction == 'SELL':
            hwnd_controls = self.sell_control_hwnds
            hwnd_index = self.sell_hwnds_index[1]
        else:
            self.logger.error('Order direction error!')

        self.logger.info(event.direction, event.symbol, event.price, event.quantity)

        time.sleep(0.1)
        setEditText(hwnd_controls[hwnd_index[0]], event.symbol)
        time.sleep(0.1)
        setEditText(hwnd_controls[hwnd_index[1]], event.price)
        time.sleep(0.1)
        setEditText(hwnd_controls[hwnd_index[2]], event.quantity)
        time.sleep(0.2)
        click(hwnd_controls[hwnd_index[3]])
        time.sleep(0.2)

    def get_orders(self):
        pass

    def check_orders(self):
        pass

    def cancel_orders(self):
        pass

    def run(self):
        while True:
            try:
                # todo
                event = self.events_in.get(False)
            except Queue.Empty:
                if datetime.now() - self.last_check_orders_time > timedelta(seconds=1):
                    if self.auto_check_orders:
                        self.check_orders()
                        # for debug only check once
                        # self.auto_check_orders = False
                    else:
                        self.logger.debug('...')
                    self.last_check_orders_time = datetime.now()

                continue
            else:
                if event is not None:
                    self.logger.debug('Got Event: event=%s', event)
                    if event.type == 'Order':
                        self.order(event)

                    elif event.type == 'CancelOrder':
                        self.cancel_order(event)

                    elif event.type == 'CheckOrders':
                        self.check_orders()
