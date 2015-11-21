import pyautogui
import threading
import Queue
from datetime import datetime
from Event import OrderStatusEvent
from utils import get_text_from_clipboard


class TradeHandler(threading.Thread):
    def __init__(self, buy_cmd, sell_cmd, cancel_cmd, check_cmd, events_in, events_out, check_order=False):
        super(TradeHandler, self).__init__()
        self._stop = threading.Event()

        self.buy_cmd = buy_cmd
        self.sell_cmd = sell_cmd
        self.cancel_cmd = cancel_cmd
        self.check_cmd = check_cmd
        self.events_in = events_in
        self.events_out = events_out
        self.check_order = check_order

        self.buyPosStock = self.get_var_pos(buy_cmd, 'stockcode')
        self.buyPosPrice = self.get_var_pos(buy_cmd, 'stockprice')
        self.buyPosVol = self.get_var_pos(buy_cmd, 'stocknum')

        self.sellPosStock = self.get_var_pos(sell_cmd, 'stockcode')
        self.sellPosPrice = self.get_var_pos(sell_cmd, 'stockprice')
        self.sellPosVol = self.get_var_pos(sell_cmd, 'stocknum')

        self.last_check_orders_time = datetime.now()

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

    def replace_var(self, cmd, event, posStock, posPrice, posVol):
        if event.type == 'Order':
            cmd[posStock][2] = event.symbol
            cmd[posPrice][2] = event.price
            cmd[posVol][2] = event.quantity

    def buy_stock(self, event):
        self.replace_var(self.buy_cmd, event, self.buyPosStock, self.buyPosPrice, self.buyPosVol)
        self.execute_cmd(self.buy_cmd)

    def sell_stock(self, event):
        self.replace_var(self.sell_cmd, event, self.sellPosStock, self.sellPosPrice, self.sellPosVol)
        self.execute_cmd(self.sell_cmd)

    def cancel_order(self, event):
        pass

    def get_orders(self):
        self.execute_cmd(self.check_cmd)
        return get_text_from_clipboard()

    def check_orders(self):
        orders = self.get_orders()
        event = OrderStatusEvent(orders)
        print(event)
        self.events_out.put(event)

    def execute_cmd(self, cmd):
        for line in cmd:
            if line[0] == 'moveto':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveTo(x, y)
                print 'move to ', x, y

            elif line[0] == 'doubleclick':
                t = float(line[1])
                pyautogui.doubleClick(interval=t)
                print 'doubleclick'

            elif line[0] == 'press':
                key = line[1]
                presses = int(line[2])
                interval = float(line[3])
                pause = float(line[4])
                pyautogui.press(key, presses=presses, interval=interval, pause=pause)
                print 'press ', line[1], presses, interval, pause

            elif line[0] == 'click':
                t = float(line[1])
                pyautogui.click(interval=t)
                print 'click'

            elif line[0] == 'type':
                t = float(line[3])
                pyautogui.typewrite(line[2])
                print 'type ', line[2]

            elif line[0] == 'hotkey':
                pyautogui.hotkey(line[1], line[2])
                print 'press ', line[1] + '+' + line[2]

            elif line[0] == 'moverel':
                x = int(line[1])
                y = int(line[2])
                t = float(line[3])
                pyautogui.moveRel(x, y, t)
                print 'move Rel ', x, y
            else:
                print 'Command error!'

    def run(self):
        while True:
            try:
                # todo
                event = self.events_in.get()
            except Queue.Empty:
                print('queue empty error!')
            else:
                if event is not None:
                    if event.type == 'Order':
                        if event.direction == 'BUY':
                            self.buy_stock(event)
                        elif event.direction == 'SELL':
                            self.sell_stock(event)
                        else:
                            print 'Order direction error!'

                    elif event.type == 'CancelOrder':
                        self.cancel_order(event)

                    elif event.type == 'CheckOrders':
                        self.check_orders()





