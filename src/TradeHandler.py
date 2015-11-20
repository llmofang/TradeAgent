import pyautogui
import threading
import Queue


class TradeHandler(threading.Thread):
    def __init__(self, buyCmd, sellCmd, cancelCmd, checkCmd, events):
        self.buyCmd = buyCmd
        self.sellCmd = sellCmd
        self.cancelCmd = cancelCmd
        self.checkCmd = checkCmd
        self.events = events

        self.buyPosStock = self.getVarPos(buyCmd, 'stockcode')
        self.buyPosPrice = self.getVarPos(buyCmd, 'stockprice')
        self.buyPosVol = self.getVarPos(buyCmd, 'stocknum')

        self.sellPosStock = self.getVarPos(sellCmd, 'stockcode')
        self.sellPosPrice = self.getVarPos(sellCmd, 'stockprice')
        self.sellPosVol = self.getVarPos(sellCmd, 'stocknum')

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
        self.replace_var(self.buyCmd, event, self.buyPosStock, self.buyPosPrice, self.buyPosVol)
        self.execute_cmd(self.buyCmd)

    def sell_stock(self, event):
        self.replace_var(self.sellCmd, event, self.sellPosStock, self.sellPosPrice, self.sellPosVol)
        self.execute_cmd(self.buyCmd)

    def cancel_order(self, event):
        pass

    def check_entrust(self, event):
        pass

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
                print 'Unkown command'

    def run(self):
        while True:
            try:
                event = self.events.get(False)
            except Queue.Empty:
                break
            else:
                if event is not None:
                    if event.type == 'Order':
                        if event.direction == 'BUY':
                            self.buy_stock(event)
                        elif event.direction == 'SELL':
                            self.sell_stock(event)

                    elif event.type == 'CancelOrder':
                        self.cancel_order(event)

                    elif event.type == 'CheckEntrust':
                        self.check_entrust(event)



