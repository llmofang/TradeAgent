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
               (self.symbol, self.direction, self.price, self.quantity)


class CancelOrderEvent(Event):
    def __init__(self, qid, entrustno=0):
        self.type = 'CancelOrder'
        self.entrustno = entrustno
        self.qid = qid

    def __str__(self):
        return "CancelOrderEvent: EntrustNo=%s, Qid = %s" % (self.entrustno, self.qid)


class CheckOrdersEvent(Event):
    def __init__(self):
        self.type = 'CheckOrders'

    def __str__(self):
        return "CheckOrdersEvent"


class OrderStatusEvent(Event):
    def __init__(self, orders):
        self.type = 'OrderStatusEvent'
        self.orders = orders

    def __str__(self):
        return "OrderStatusEvent: Content = %s" % self.orders


class NewOrdersEvent(Event):
    def __init__(self, new_orders, update_kdb):
        self.type = 'NewOrdersEvent'
        self.new_orders = new_orders
        self.update_kdb = update_kdb

    def __str__(self):
        return "OrderStatusEvent: NewOrders = %s" % self.new_orders

