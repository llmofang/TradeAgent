from Queue import Queue
from TradeHandler import TradeHandler
from RequestHandler import RequestHandler
from ResponseHandler import ResponseHandler
from qpython import qconnection
from utils import read_commands
import sys

BUY_LIST_FILE = 'buyStockList.txt'
SELL_LIST_FILE = 'sellStockList.txt'
CHECK_LIST_FILE = 'checkStockList.txt'
CANCEL_LIST_FILE = 'cancelOrderList.txt'

buy_cmd = read_commands(BUY_LIST_FILE)
sell_cmd = read_commands(SELL_LIST_FILE)
cancel_cmd = read_commands(CANCEL_LIST_FILE)
check_cmd = read_commands(CHECK_LIST_FILE)

events_trade = Queue()
events_response = Queue()
q_host = "183.136.130.82"
q_port = "5030"
q_request_table = 'trade1'
q_response_table = 'trade2'
q_sub_users = []

q_req = qconnection.QConnection(host='183.136.130.82', port=5030, pandas=True)
q_res = qconnection.QConnection(host='183.136.130.82', port=5030, pandas=True)

try:
    q_req.open()
    q_res.open()

    trade_handler = TradeHandler(buy_cmd, sell_cmd, cancel_cmd, check_cmd, events_trade, events_response, True)
    request_handler = RequestHandler(q_req, events_response, events_trade, q_request_table, q_sub_users)
    response_handler = ResponseHandler(q_res, events_response, q_response_table)

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
