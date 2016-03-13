import multiprocessing
from TradeHandler import TradeHandler
from RequestHandler import RequestHandler
from ZXResponseHandler import ZXResponseHandler
from HTResponseHandler import HTResponseHandler

import sys
import logging
import logging.config
import getopt
import ConfigParser


cf = ConfigParser.ConfigParser()
cf.read("tradeagent.conf")
broker = cf.get("cmd_mode", "broker")



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

    events_trade = multiprocessing.Manager().Queue()
    events_response = multiprocessing.Manager().Queue()

    trade_handler = TradeHandler(events_trade, events_response, check)
    request_handler = RequestHandler(events_response, events_trade, events_types)
    if broker == 'ht':
        response_handler = HTResponseHandler(events_response)
    elif broker == 'zx':
        response_handler = ZXResponseHandler(events_response)
    else:
        logger.error('Unknown broker =%s', broker)

    response_handler.start()
    trade_handler.start()
    request_handler.start()

    trade_handler.join()
    request_handler.join()
    response_handler.join()

if __name__ == '__main__':
    main(sys.argv)