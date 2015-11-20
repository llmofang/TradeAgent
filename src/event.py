class Event(object):
    pass

class OrderEvent(Event):
    def __init__(self, symbol, direction, price, quantity):
        self.type = 'Order'
        self.symbol = symbol
        self.direction = direction
        self.price = price
        self.quantity = quantity

    def __str__(self):
        return "OrderEvent: Symbol=%s, Direction=%s, Price=%s, quantity=%s" % \
              (self.symbol, self.direction, self.price, self.quantity))

class CancelOrderEvent(Event):
    def __init__(self, entrustNo):
        self.type = 'CancelOrder'
        self.entrustNo = entrustNo

    def __str__(self):
        return "CancelOrderEvent: EntrustNo=%s" % self.entrustNo

class CheckEntrustEvent(Event):
    def __init__(self):
        self.type = 'CheckEntrust'

    def __str__(self):
        return "CheckEntrustEvent"
