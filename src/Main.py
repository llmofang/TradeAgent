from Queue import Queue
from TradeHandler import TradeHandler
from RequestHandler import RequestHandler
from ZXResponseHandler import ZXResponseHandler
from HTResponseHandler import HTResponseHandler
from W32TradeHandler import W32TradeHandler

from qpython import qconnection
from MyUtils import read_commands
import sys
import logging
import logging.config
import getopt
import ConfigParser


cf = ConfigParser.ConfigParser()
cf.read("tradeagent.conf")

broker = cf.get("cmd_mode", "broker")

rz_stocks = cf.get("stocks", "rz_stocks").split(',')

buy_cmd_file = cf.get("cmd_mode", "buy_cmd_file")
sell_cmd_file = cf.get("cmd_mode", "sell_cmd_file")
rz_buy_cmd_file = cf.get("cmd_mode", "rz_buy_cmd_file")
rz_sell_cmd_file = cf.get("cmd_mode", "rz_sell_cmd_file")
check_cmd_file = cf.get("cmd_mode", "check_cmd_file")
cancel_cmd_file = cf.get("cmd_mode", "cancel_cmd_file")



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

    buy_cmd = read_commands(buy_cmd_file)
    sell_cmd = read_commands(sell_cmd_file)
    rz_buy_cmd = read_commands(rz_buy_cmd_file)
    rz_sell_cmd = read_commands(rz_sell_cmd_file)
    cancel_cmd = read_commands(cancel_cmd_file)
    check_cmd = read_commands(check_cmd_file)

    events_trade = Queue()
    events_response = Queue()
    q_host = cf.get("db", "host")
    q_port = cf.getint("db", "port")
    q_request_table = cf.get("db", "request_table")
    q_response_table = cf.get("db", "response_table")
    # todo
    q_sub_users = cf.get("kdb", "sub_users").split(',')
    q_var_prefix = cf.get("kdb", "var_prefix")

    mode_style = cf.get("mode", "style")

    wnd_title = cf.get("hwnd_mode", "wnd_title")
    hwnd_parents_buy = cf.get("hwnd_mode", "hwnd_parents_buy").split(',')
    hwnd_controls_buy = cf.get("hwnd_mode", "hwnd_controls_buy").split(',')
    hwnd_parents_sell = cf.get("hwnd_mode", "hwnd_parents_sell").split(',')
    hwnd_controls_sell = cf.get("hwnd_mode", "hwnd_controls_sell").split(',')

    hwnd_parents_buy = [int(i) for i in hwnd_parents_buy]
    hwnd_controls_buy = [int(i) for i in hwnd_controls_buy]
    hwnd_parents_sell = [int(i) for i in hwnd_parents_sell]
    hwnd_controls_sell = [int(i) for i in hwnd_controls_sell]

    buy_hwnds_index = []
    sell_hwnds_index = []

    buy_hwnds_index.append(hwnd_parents_buy)
    buy_hwnds_index.append(hwnd_controls_buy)

    sell_hwnds_index.append(hwnd_parents_sell)
    sell_hwnds_index.append(hwnd_controls_sell)

    q_req = qconnection.QConnection(host=q_host, port=q_port, pandas=True)
    q_res = qconnection.QConnection(host=q_host, port=q_port, pandas=True)

    try:
        q_req.open()
        q_res.open()

        if mode_style == 'hwnd':
            trade_handler = W32TradeHandler(wnd_title, buy_hwnds_index, sell_hwnds_index, buy_hwnds_index,
                                           sell_hwnds_index, rz_stocks, events_trade, logger)
        else:
            trade_handler = TradeHandler(buy_cmd, sell_cmd, rz_buy_cmd, rz_sell_cmd, rz_stocks, cancel_cmd, check_cmd,
                                        events_trade, events_response, logger, check)

        request_handler = RequestHandler(q_req, events_response, events_trade, q_request_table, q_sub_users,
                                         logger, events_types)
        if broker == 'ht':
            response_handler = HTResponseHandler(q_res, events_response, q_response_table, logger, q_var_prefix)
        elif broker == 'zx':
            response_handler = ZXResponseHandler(q_res, events_response, q_response_table, logger, q_var_prefix)
        else:
            logger.error('Unknown broker =%s', broker)

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