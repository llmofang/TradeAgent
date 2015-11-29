from Queue import Queue
from TradeHandler import TradeHandler
from RequestHandler import RequestHandler
from ResponseHandler import ResponseHandler
from qpython import qconnection
from MyUtils import read_commands
import sys
import logging
import logging.config
import getopt

BUY_LIST_FILE = 'cmd\\buyStockList.txt'
SELL_LIST_FILE = 'cmd\\sellStockList.txt'
CHECK_LIST_FILE = 'cmd\\checkStockList.txt'
CANCEL_LIST_FILE = 'cmd\\cancelOrderList.txt'


def usage():
    print 'Main.py usage:'
    print '-h: print help message.'
    print '-v: print script version'
    print '-c: support cancel orders'
    print '-o: support order'
    print '-k: support check orders'


def version():
    print 'Main.py 1.0.0'


def main(argv):
    cancel = False
    order = False
    check = False
    try:
        opts, args = getopt.getopt(argv[1:], ':hvcok', [])
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in '-h':
            usage()
            sys.exit(1)
        elif opt in '-v':
            version()
            sys.exit(0)
        elif opt in '-c':
            cancel = True
        elif opt in '-o':
            order = True
        elif opt in '-k':
            check = True
        else:
            print 'unhandled option'
            sys.exit(3)
    run(cancel, check, order)


def run(cancel, check, order):
    events_types = []
    if cancel:
        events_types.append('CancelOrderEvent')
    if order:
        events_types.append('OrderEvent')

    logging.config.fileConfig('log.conf')
    logger = logging.getLogger('main')
    logger.debug('check=%s', check)
    logger.debug('events_types=%s', events_types)

    buy_cmd = read_commands(BUY_LIST_FILE)
    sell_cmd = read_commands(SELL_LIST_FILE)
    cancel_cmd = read_commands(CANCEL_LIST_FILE)
    check_cmd = read_commands(CHECK_LIST_FILE)

    events_trade = Queue()
    events_response = Queue()
    q_host = '183.136.130.82'
    q_port = 15030
    q_request_table = 'trade1'
    q_response_table = 'trade2'
    q_sub_users = []

    q_req = qconnection.QConnection(host=q_host, port=q_port, pandas=True)
    q_res = qconnection.QConnection(host=q_host, port=q_port, pandas=True)

    try:
        q_req.open()
        q_res.open()

        trade_handler = TradeHandler(buy_cmd, sell_cmd, cancel_cmd, check_cmd,
                                     events_trade, events_response, logger, check)
        request_handler = RequestHandler(q_req, events_response, events_trade, q_request_table, q_sub_users,
                                         logger, events_types)
        response_handler = ResponseHandler(q_res, events_response, q_response_table, logger)

        response_handler.start()
        trade_handler.start()
        request_handler.start()

        sys.stdin.readline()

        trade_handler.stop()
        request_handler.stop()
        response_handler.stop()

    finally:
        q_req.close()
        q_res.close()

if __name__ == '__main__':
    main(sys.argv)