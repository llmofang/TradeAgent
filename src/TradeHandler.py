# -*- coding: cp936 -*-
import pyautogui
import threading
import Queue
from datetime import datetime
from datetime import timedelta
from Event import OrderStatusEvent
import pandas as pd


class TradeHandler(threading.Thread):
    def __init__(self, buy_cmd, sell_cmd, cancel_cmd, check_cmd, events_in, events_out, logger, auto_check_orders=False):
        super(TradeHandler, self).__init__()
        self._stop = threading.Event()

        self.buy_cmd = buy_cmd
        self.sell_cmd = sell_cmd
        self.cancel_cmd = cancel_cmd
        self.check_cmd = check_cmd
        self.events_in = events_in
        self.events_out = events_out
        self.auto_check_orders = auto_check_orders

        self.buyPosStock = self.get_var_pos(buy_cmd, 'stockcode')
        self.buyPosPrice = self.get_var_pos(buy_cmd, 'stockprice')
        self.buyPosVol = self.get_var_pos(buy_cmd, 'stocknum')

        self.sellPosStock = self.get_var_pos(sell_cmd, 'stockcode')
        self.sellPosPrice = self.get_var_pos(sell_cmd, 'stockprice')
        self.sellPosVol = self.get_var_pos(sell_cmd, 'stocknum')

        self.cancelPosEntrust = self.get_var_pos(cancel_cmd, 'entrustno')

        self.last_check_orders_time = datetime.now()
        self.logger = logger

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def get_var_pos(self, cmd, key):
        i = 0
        for line in cmd:
            if line[1] == key:
                return i
            i += 1
        return -1

    def replace_order_var(self, cmd, event, posStock, posPrice, posVol):
        if event.type == 'Order':
            cmd[posStock][2] = event.symbol
            cmd[posPrice][2] = event.price
            cmd[posVol][2] = event.quantity

    def replace_cancel_var(self, cmd, event, posEntrust):
        if event.type == 'CancelOrder':
            cmd[posEntrust][2] = event.entrustno

    def buy_stock(self, event):
        self.replace_order_var(self.buy_cmd, event, self.buyPosStock, self.buyPosPrice, self.buyPosVol)
        self.execute_cmd(self.buy_cmd)

    def sell_stock(self, event):
        self.replace_order_var(self.sell_cmd, event, self.sellPosStock, self.sellPosPrice, self.sellPosVol)
        self.execute_cmd(self.sell_cmd)

    def cancel_order(self, event):
        if event.entrustno > 0:
            self.replace_cancel_var(self.cancel_cmd, event, self.cancelPosEntrust)
            self.execute_cmd(self.cancel_cmd)
        elif event.qid != 0:
            # TODO
            pass
        else:
            print('Unvalid cancel order command!')

    def get_orders(self):
        self.execute_cmd(self.check_cmd)
        orders = []
        try:
            orders = pd.read_clipboard(encoding='gbk', parse_dates=[u'委托时间'])
        except Exception, e:
            print(e)
        finally:
            return orders

    def check_orders(self):
        orders = self.get_orders()
        if len(orders) > 0:
            event = OrderStatusEvent(orders)
            self.events_out.put(event)

    def execute_cmd(self, cmd):
        for line in cmd:
            if line[0] == 'moveto':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveTo(x, y)
                self.logger.debug('move to (%i, %i)', x, y)

            elif line[0] == 'doubleclick':
                t = float(line[1])
                pyautogui.doubleClick(interval=t)
                self.logger.debug('doubleclick')

            elif line[0] == 'rightclick':
                t = float(line[1])
                pyautogui.rightClick()
                self.logger.debug('rightClick')

            elif line[0] == 'press':
                key = line[1]
                presses = int(line[2])
                interval = float(line[3])
                pause = float(line[4])
                pyautogui.press(key, presses=presses, interval=interval, pause=pause)
                self.logger.debug('press key=%s, presses=%i, interval=%f, pause=%f ', key, presses, interval, pause)

            elif line[0] == 'click':
                t = float(line[1])
                pyautogui.click(interval=t)
                self.logger.debug('click')

            elif line[0] == 'type':
                t = float(line[3])
                pyautogui.typewrite(line[2])
                self.logger.debug('type %s', line[2])

            elif line[0] == 'hotkey':
                pyautogui.hotkey(line[1], line[2])
                self.logger.debug('press hotkey=%s', line[1] + '+' + line[2])

            elif line[0] == 'moverel':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveRel(x, y, t)
                self.logger.debug('move Rel (%i, %i)', x, y)
            else:
                self.logger.error('AutoGui Command error!')

    def run(self):
        while True:
            try:
                # todo
                event = self.events_in.get(False)
            except Queue.Empty:
                if self.auto_check_orders:
                    if datetime.now() - self.last_check_orders_time > timedelta(seconds=2):
                        self.check_orders()
                        self.last_check_orders_time = datetime.now()
                        # for debug only check once
                        # self.auto_check_orders = False
                continue
            else:
                if event is not None:
                    self.logger.debug('Got Event: event=%s', event)
                    if event.type == 'Order':
                        if event.direction == 'BUY':
                            self.buy_stock(event)
                        elif event.direction == 'SELL':
                            self.sell_stock(event)
                        else:
                            self.logger.error('Order direction error!')

                    elif event.type == 'CancelOrder':
                        self.cancel_order(event)

                    elif event.type == 'CheckOrders':
                        self.check_orders()