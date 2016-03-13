# -*- coding: utf-8 -*-
import pyautogui
import multiprocessing
import Queue
from datetime import datetime
from datetime import timedelta
from Event import OrderStatusEvent
import pandas as pd
import ConfigParser
from MyUtils import read_commands


class TradeHandler(multiprocessing.Process):
    def __init__(self, events_in, events_out, auto_check_orders=False):
        super(TradeHandler, self).__init__()

        self.events_in = events_in
        self.events_out = events_out

        cf = ConfigParser.ConfigParser()
        cf.read("tradeagent.conf")

        self.buy_cmd = read_commands(cf.get("cmd_mode", "buy_cmd_file"))
        self.sell_cmd = read_commands(cf.get("cmd_mode", "sell_cmd_file"))

        self.rz_buy_cmd = read_commands(cf.get("cmd_mode", "rz_buy_cmd_file"))
        self.rz_sell_cmd = read_commands(cf.get("cmd_mode", "rz_sell_cmd_file"))

        self.rz_stocks = cf.get("stocks", "rz_stocks").split(',')

        self.cancel_cmd = read_commands(cf.get("cmd_mode", "cancel_cmd_file"))
        self.check_cmd = read_commands(cf.get("cmd_mode", "check_cmd_file"))
        self.events_in = multiprocessing.Manager().Queue()
        self.events_out = multiprocessing.Manager().Queue()
        self.auto_check_orders = auto_check_orders

        self.buyPosStock = self.get_var_pos(self.buy_cmd, 'stockcode')
        self.buyPosPrice = self.get_var_pos(self.buy_cmd, 'stockprice')
        self.buyPosVol = self.get_var_pos(self.buy_cmd, 'stocknum')

        self.sellPosStock = self.get_var_pos(self.sell_cmd, 'stockcode')
        self.sellPosPrice = self.get_var_pos(self.sell_cmd, 'stockprice')
        self.sellPosVol = self.get_var_pos(self.sell_cmd, 'stocknum')

        self.rzbuyPosStock = self.get_var_pos(self.rz_buy_cmd, 'stockcode')
        self.rzbuyPosPrice = self.get_var_pos(self.rz_buy_cmd, 'stockprice')
        self.rzbuyPosVol = self.get_var_pos(self.rz_buy_cmd, 'stocknum')

        self.rzsellPosStock = self.get_var_pos(self.rz_sell_cmd, 'stockcode')
        self.rzsellPosPrice = self.get_var_pos(self.rz_sell_cmd, 'stockprice')
        self.rzsellPosVol = self.get_var_pos(self.rz_sell_cmd, 'stocknum')

        self.cancelPosEntrust = self.get_var_pos(self.cancel_cmd, 'entrustno')

        self.last_check_orders_time = datetime.now()

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
        if event.symbol in self.rz_stocks:
            print(u'融资买入股票: %s', event.symbol)
            self.replace_order_var(self.rz_buy_cmd, event, self.rzbuyPosStock, self.rzbuyPosPrice, self.rzbuyPosVol)
            self.execute_cmd(self.rz_buy_cmd)
        else:
            print(u'买入股票: %s', event.symbol)
            self.replace_order_var(self.buy_cmd, event, self.buyPosStock, self.buyPosPrice, self.buyPosVol)
            self.execute_cmd(self.buy_cmd)

    def sell_stock(self, event):
        if event.symbol in self.rz_stocks:
            print(u'融资卖出股票: %s', event.symbol)
            self.replace_order_var(self.rz_sell_cmd, event, self.rzsellPosStock, self.rzsellPosPrice, self.rzsellPosVol)
            self.execute_cmd(self.rz_sell_cmd)
        else:
            print(u'卖出股票: %s', event.symbol)
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

    def check_mouse_position(self):
        while True:
            X, Y = pyautogui.size()
            x, y = pyautogui.position()
            if (x < X * 0.8) and (y < Y * 0.8):
                break

    def get_orders(self):
        self.check_mouse_position()
        self.execute_cmd(self.check_cmd)
        new_orders = pd.DataFrame([])
        try:
            new_orders = pd.read_clipboard(encoding='gbk', parse_dates=[u'委托时间'], nrows=30)
            print('got new orders from clipboard, new_orders = %s', new_orders.to_string())
            if len(new_orders) > 0:
                new_orders = new_orders.set_index([u'委托时间'])
                new = datetime.now() + timedelta(minutes=5)
                old = datetime.now() - timedelta(minutes=90)
                new_orders = new_orders.between_time(old, new)
                print('get recent orders: new_orders = %s', new_orders.to_string())

                if len(new_orders) > 0:
                    columns_drop = [u'委托日期', u'证券名称', u'委托类型', u'资金帐号', u'交易市场', u'股东账户' u'返回信息', 'Unnamed: 16', 'Unnamed: 17']
                    for column in columns_drop:
                        if column in new_orders.columns:
                            print('droping unused columns: column=%s', column)
                            new_orders = new_orders.drop(column, axis=1)
                else:
                    new_orders = pd.DataFrame([])

        except Exception, e:
            print(e)
        finally:
            return new_orders

    def check_orders(self):
        orders = self.get_orders()
        try:
            if len(orders) > 0:
                event = OrderStatusEvent(orders)
                print('generate OrderStatusEvent=%s', event)
                self.events_out.put(event)
                qsize = self.events_out.qsize()
                if qsize > 3:
                    print('events queue size is too large: qsize=%i', qsize)
                else:
                    print('events queue size: qsize=%i', qsize)
        except Exception, e:
            print(e)

    def execute_cmd(self, cmd):
        for line in cmd:
            if line[0] == 'moveto':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveTo(x, y, t)
                print('move to (%i, %i) duration=%f', x, y, t)

            elif line[0] == 'click':
                try:
                    pyautogui.click()
                    print('click')
                except Exception, e:
                    print(e)

            elif line[0] == 'rightclick':
                pyautogui.rightClick()
                print('rightClick')

            elif line[0] == 'doubleclick':
                t = float(line[1])
                pyautogui.doubleClick(interval=t)
                print('doubleclick')

            elif line[0] == 'press':
                key = line[1]
                presses = int(line[2])
                interval = float(line[3])
                pause = float(line[4])
                pyautogui.press(key, presses=presses, interval=interval, pause=pause)
                print('press key=%s, presses=%i, interval=%f, pause=%f ', key, presses, interval, pause)

            elif line[0] == 'type':
                t = float(line[3])
                pyautogui.typewrite(line[2])
                print('type %s, interval=%f', line[2], t)

            elif line[0] == 'hotkey':
                pyautogui.hotkey(line[1], line[2])
                print('press hotkey=%s', line[1] + '+' + line[2])

            elif line[0] == 'moverel':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveRel(x, y, t)
                print('move Rel (%i, %i)', x, y)
            else:
                print('AutoGui Command error!')

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
                        print('...')
                    self.last_check_orders_time = datetime.now()

                continue
            else:
                if event is not None:
                    print('Got Event: event=%s', event)
                    if event.type == 'Order':
                        if event.direction == 'BUY':
                            self.buy_stock(event)
                        elif event.direction == 'SELL':
                            self.sell_stock(event)
                        else:
                            print('Order direction error!')

                    elif event.type == 'CancelOrder':
                        self.cancel_order(event)

                    elif event.type == 'CheckOrders':
                        self.check_orders()
