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
    def __init__(self, entrust_no):
        self.type = 'CancelOrder'
        self.entrust_no = entrust_no

    def __str__(self):
        return "CancelOrderEvent: EntrustNo=%s" % self.entrustNo


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
        return "OrderStatusEvent: Content = %s" % self.content

class NewOrdersEvent(Event):
    def __init__(self, new_orders):
        self.type = 'NewOrdersEvent'
        self.new_orders = new_orders

    def __str__(self):
        return "OrderStatusEvent: NewOrders = %s" % self.new_orders

