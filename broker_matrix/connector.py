#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
one-thread IB connection
"""

import time
from datetime import datetime
from threading import Thread, Lock
import pytz

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.ticktype import TickTypeEnum

from lib2.remote import open_remote_port, close_remote_port
from .errors import reconnect_errors

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
DEFAULT_IP = "127.0.0.1"
DEFAULT_PORT = 4002

def call_handlers_list(handlers, the_data):
    if isinstance(handlers, list):
        for handler in handlers:
            handler(the_data)
    else:
        handlers(the_data)

def tick_type_to_str(tick_type):
    return TickTypeEnum.idx2name.get(tick_type, "NOTFOUND")

class Connector:
    def __init__(self, client_id, remote=None, local_ip=DEFAULT_IP, local_port=DEFAULT_PORT, host=None, port=None):
        self.client_id = client_id
        self.remote = remote
        self.host = host
        self.port = port
        self.server = None
        self.ib_port = port if port else local_port
        self.local_ip = local_ip if remote else host
        self.broker_api = None
        self._thread = None
        self.req_id = None
        self.lock = Lock()

    def start(self, timeout=5):
        if not self.remote is None:
            self.server, self.ib_port = open_remote_port(remote=self.remote, host=self.host, port=self.port)

        if self.broker_api is None:
            self.broker_api = IBapi()
        else:
            self.broker_api = IBapi(self.broker_api.requests, self.broker_api.special_requests, self.broker_api.requests_executions, self.broker_api.order_post_process_unspecified_commission)
        self.broker_api.connect(self.local_ip, self.ib_port, self.client_id)
        self._thread = Thread(target=self.broker_api.run)
        self._thread.start()

        counter = 0
        while not isinstance(self.broker_api.next_order_id, int):
            counter += 1
            time.sleep(1)
            if counter == timeout:
                print("can't connect to IB")
                self.stop()
                break
        else:
            self.req_id = self.broker_api.next_order_id

    def stop(self):
        if not self.broker_api is None:
            self.broker_api.disconnect()
        if not self.remote is None:
            close_remote_port(self.server)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def add_request(self, request):
        self.broker_api.requests[request.request_id] = request

    def get_special_request(self, request_type):
        if request_type in self.broker_api.special_requests:
            if self.broker_api.special_requests[request_type].is_active():
                return self.broker_api.special_requests[request_type]
        return None

    def set_special_request(self, request_type, request):
        self.broker_api.special_requests[request_type] = request

    def next_req_id(self):
        with self.lock:
            self.req_id += 1
        return self.req_id

    def set_unspecified_commission_process(self, order_post_process):
        self.broker_api.order_post_process_unspecified_commission = order_post_process

class IBapi(EWrapper, EClient):
    def __init__(self, old_requests=None, special_requests=None, requests_executions=None, order_post_process_unspecified_commission=None):
        EClient.__init__(self, self)
        EWrapper.__init__(self)
        self.next_order_id = None
        self.requests = old_requests if not old_requests is None else {}
        self.special_requests = special_requests if not special_requests is None else {}
        self.requests_executions = requests_executions if not requests_executions is None else {}
        self.order_post_process_unspecified_commission = order_post_process_unspecified_commission
        self.needs_reconnect = False
        self.last_data_time = datetime.now()
        self.connection_check = False

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        if errorCode in reconnect_errors():
            self.needs_reconnect = True
            print("error:", reqId, errorCode, errorString, flush=True)
            return

        if reqId not in self.requests:
            print("error:", reqId, errorCode, errorString, flush=True)
            return

        print("error:", reqId, errorCode, errorString, flush=True)  #TODO remove temporary
        self.requests[reqId].errors[datetime.now().astimezone(EASTERN)] = (errorCode, errorString)
        if not self.requests[reqId].on_error is None:
            self.requests[reqId].on_error(self.requests[reqId], errorCode)
        # if self.requests[reqId].request_type in["reqHistoricalData", "reqContractDetails", "reqSecDefOptParams"]:

    def tickPrice(self, reqId, tickType, price, attrib):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data((self.last_data_time.astimezone(EASTERN).replace(tzinfo=None),
                                          TickTypeEnum.to_str(tickType), price), "price")

    def tickSize(self, reqId, tickType, size):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data((self.last_data_time.astimezone(EASTERN).replace(tzinfo=None),
                                          TickTypeEnum.to_str(tickType), size), "size")

    def historicalData(self, reqId, bar):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        ts = int(bar.date)
        # IB Gateway bar.date for daily data is concatenation year+month+day
        # for intraday data it is UNIX timestamp
        # comparing with 1E9 works for dates after 2001-09-09 (IB Gateway provides data not older than 5y)
        if ts < 1000000000:
            bar_date = datetime(ts//10000, ts%10000//100, ts%100)
        else:
            bar_date = datetime.fromtimestamp(int(bar.date))

        self.requests[reqId].on_get_data((bar_date,
                                          bar.open,
                                          bar.high,
                                          bar.low,
                                          bar.close,
                                          bar.volume))

    def historicalDataEnd(self, reqId, start, end):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_finished is None:
            return
        self.requests[reqId].on_finished(self.requests[reqId])

    # def historicalDataUpdate(self, reqId, bar):
    #     print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)

    def nextValidId(self, orderId: int):
        self.next_order_id = orderId

    def contractDetails(self, reqId, contractDetails):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data(contractDetails)
        self.requests[reqId].on_finished(self.requests[reqId])

    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data({"exchange": exchange,
                                          "underlyingConId": underlyingConId,
                                          "tradingClass": tradingClass,
                                          "multiplier": multiplier,
                                          "expirations": expirations,
                                          "strikes": strikes})

    def securityDefinitionOptionParameterEnd(self, reqId):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_finished is None:
            return
        self.requests[reqId].on_finished(self.requests[reqId])

    def position(self, account, contract, position, avgCost):
        self.last_data_time = datetime.now()
        request = self.special_requests["reqPositions"]
        if request.on_get_data is None:
            return
        self.requests[request.request_id].on_get_data((account,
                                                       contract.symbol,
                                                       contract.strike,
                                                       contract.secType,
                                                       contract.lastTradeDateOrContractMonth,
                                                       position,
                                                       avgCost,
                                                       contract))

    def positionEnd(self):
        self.last_data_time = datetime.now()
        request = self.special_requests["reqPositions"]
        if request.on_finished is None:
            return
        self.requests[request.request_id].on_finished(request)

    def positionMulti(self, reqId, account, modelCode, contract, pos, avgCost):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data((account,
                                          contract.symbol,
                                          contract.strike,
                                          contract.secType,
                                          contract.lastTradeDateOrContractMonth,
                                          pos,
                                          avgCost,
                                          contract,
                                          modelCode))

    def positionMultiEnd(self, reqId):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_finished is None:
            return
        self.requests[reqId].on_finished(self.requests[reqId])

    def openOrder(self, orderId, contract, order, orderState):
        self.last_data_time = datetime.now()
        # print("openOrder", orderId, orderState)
        if "reqOpenOrders" not in self.special_requests:
            return
        request = self.special_requests["reqOpenOrders"]
        if request.on_get_data is None:
            return
        self.requests[request.request_id].on_get_data((orderId, contract, order, orderState))

    def openOrderEnd(self):
        self.last_data_time = datetime.now()
        if "reqOpenOrders" not in self.special_requests:
            return
        request = self.special_requests["reqOpenOrders"]
        if request.on_finished is None:
            return
        self.requests[request.request_id].on_finished(request)

    def execDetails(self, reqId, contract, execution):
        self.last_data_time = datetime.now()
        print("execDetails: ", execution)
        orderId = execution.orderId
        if not orderId in self.requests or self.requests[orderId].on_get_data is None:
            return
        self.requests_executions[execution.execId] = orderId
        self.requests[orderId].on_get_data(execution, "execution_details")

    def commissionReport(self, commissionReport):
        self.last_data_time = datetime.now()
        print("commissionReport: ", type(commissionReport), " ", commissionReport)
        if not commissionReport.execId in self.requests_executions:
            self.order_post_process_unspecified_commission(None, commissionReport, "commission")
            return
        reqId = self.requests_executions[commissionReport.execId]
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data(commissionReport, "commission")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        self.last_data_time = datetime.now()
        print("orderStatus: ", orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        if not orderId in self.requests or self.requests[orderId].on_get_data is None:
            return
        self.requests[orderId].on_get_data((self.last_data_time.astimezone(EASTERN).replace(tzinfo=None),
                                            status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice), "order_status")
# TODO set order finished

    def accountSummary(self, reqId, account, tag, value, currency):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_get_data is None:
            return
        self.requests[reqId].on_get_data((account,
                                          tag,
                                          value,
                                          currency))

    def accountSummaryEnd(self, reqId):
        self.last_data_time = datetime.now()
        if not reqId in self.requests or self.requests[reqId].on_finished is None:
            return
        self.requests[reqId].on_finished(self.requests[reqId])

    def managedAccounts(self, accountsList):
        self.last_data_time = datetime.now()
        self.connection_check = False
        if "reqManagedAccts" not in self.special_requests:
            print(accountsList)
            return
        request = self.special_requests["reqManagedAccts"]
        if request.on_get_data is None:
            return
        self.requests[request.request_id].on_get_data((accountsList, ))
        request.on_finished(request)
