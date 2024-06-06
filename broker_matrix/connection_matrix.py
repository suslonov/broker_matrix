#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
IB multi-connection manager
"""

from datetime import datetime, timedelta
import time
import random
from threading import Thread, Lock
from queue import Queue
import pytz
import pandas as pd
from numpy import sqrt

from .connector import Connector
from .requests import Request, MarketDataStreamRequest, OrderRequest, MAX_REQUESTS, REQUEST_CALLS, REQUEST_CANCEL_CALLS
from .errors import request_warnings

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC

START_CLIENT_ID = 123001
STANDARD_TIMEOUT = 20
MARKET_REQUEST_TIMEOUT = 300
POSITION_TIMEOUT = 60
MANAGED_ACCTS_TIMEOUT = 30

class RequestRecord():
    def __init__(self):
        self.count = 0
        self.last = 0
        self.lock = Lock()
        self.queue = Queue()

class ConnectionMatrix():
    def __init__(self, max_requests=None, client_id=None):
        if max_requests:
            self.max_requests = max_requests
        else:
            self.max_requests = MAX_REQUESTS
        self.connectors = {}
        self.request_types = {}
        self.global_requests = {}
        self.market_requests_by_symbol = {}
        self.buy_orders_by_symbol = {}
        self.sell_orders_by_symbol = {}
        self.stop_orders_by_symbol = {}
        if client_id:
            self.client_id = client_id
        else:
            self.client_id = START_CLIENT_ID
        self.request_counters = {request_type: RequestRecord() for request_type in self.max_requests}
        self._thread = None
        self.run_thread = True

    def start(self):
        self._thread = Thread(target=self.queue_and_timeout_thread)
        self.run_thread = True
        self._thread.start()

    def stop(self):
        self.run_thread = False
        self.close_all_connections()

    def queue_and_timeout_thread(self):
        while self.run_thread:
            for request_type in self.max_requests:
                if self.request_counters[request_type].count < self.max_requests[request_type] and self.request_counters[request_type].queue.qsize() > 0:
                    with self.request_counters[request_type].lock:
                        while self.request_counters[request_type].count < self.max_requests[request_type] and self.request_counters[request_type].queue.qsize() > 0:
                            (connector_id, request_id) = self.request_counters[request_type].queue.get()
                            getattr(self, REQUEST_CALLS[request_type])(self.global_requests[(connector_id, request_id)])
                            self.request_counters[request_type].count += 1
                            self.request_counters[request_type].last = request_id
            now_time = datetime.now()
            for request in self.global_requests.values():
                if request.request_type in self.max_requests:
                    if (request.is_active()
                        and (request.is_busy is None or not request.is_busy(request))
                        and not request.timeout_time is None
                        and request.timeout_time < now_time
                        and (self.request_counters[request.request_type].count >= self.max_requests[request.request_type] - 1
                             or self.request_counters[request.request_type].queue.qsize() > 0)
                        ):
                        with self.request_counters[request.request_type].lock:
                            getattr(self, REQUEST_CANCEL_CALLS[request.request_type])(request, "TimedOut", no_lock=True)
                            self.request_counters[request.request_type].count -= 1
                else:
                    if (request.is_active()
                        and (request.is_busy is None or not request.is_busy(request))
                        and not request.timeout_time is None
                        and request.timeout_time < now_time
                        ):
                        if not REQUEST_CANCEL_CALLS[request.request_type] is None:
                            cancel_call = getattr(self, REQUEST_CANCEL_CALLS[request.request_type])
                            cancel_call(request, "TimedOut")
            time.sleep(1)

    def check_request_in_the_queue(self, request_type, request_symbol):
        for r in self.global_requests:
            if self.global_requests[r].request_type == request_type and self.global_requests[r].request_id > self.request_counters[request_type].last and self.global_requests[r].request_symbol() == request_symbol:
                return True
        return False

    def create_connection(self, request_types, client_id=None, remote="aws_ib", host=None, port=None):
        # if remote is None:
            # remote="aws_ib"
        if client_id:
            self.client_id = client_id
        else:
            client_id = self.client_id
        self.connectors[client_id] = Connector(client_id=client_id, remote=remote, host=host, port=port)
        for request_type in request_types:
            if request_type in self.request_types:
                self.request_types[request_type].append(client_id)
            else:
                self.request_types[request_type] = [client_id]
        self.connectors[client_id].start()
        self.client_id += 1
        return client_id

    def close_connection(self, client_id):
        self.connectors[client_id].stop()
        for request_type in self.request_types:
            if client_id in self.request_types[request_type]:
                self.request_types[request_type].remove(client_id)

    def close_all_connections(self):
        for client_id in self.connectors:
            self.close_connection(client_id)

    def get_all_connection_statuses(self):
        return {client_id: self.connectors[client_id].broker_api.isConnected() for client_id in self.connectors}

    def set_unspecified_commission_process(self, order_post_process):
        for connector in self.connectors.values():
            connector.set_unspecified_commission_process(order_post_process)

    def broker_api_selector(self, request_type):
        if not request_type in self.request_types:
            return None
        if len(self.request_types[request_type]) == 0:
            return None
        if len(self.request_types[request_type]) == 1:
            client_id = self.request_types[request_type][0]
        else:
            client_id = random.choice(self.request_types[request_type])
        return client_id, self.connectors[client_id]

    def active_requests(self):
        for request in self.global_requests.values():
            if request.is_active():
                yield request

    def request_set_finished(self, request):
        if request.request_type in self.max_requests:
            with self.request_counters[request.request_type].lock:
                if not request.properties["TimedOut"]:
                    self.request_counters[request.request_type].count -= 1
        request.set_finished()

    def request_set_cancelled_error(self, request, errorCode):
        if request.request_type in self.max_requests:
            with self.request_counters[request.request_type].lock:
                if not request.properties["TimedOut"]:
                    self.request_counters[request.request_type].count -= 1
        if errorCode in request_warnings():
            return
        request.set_cancelled("Error")
        if request.request_type == "placeOrder" and not request.child_order_id is None and self.global_requests[(request.connector_id, request.child_order_id)].is_active():
            self.req_cancel_order(self.global_requests[(request.connector_id, request.child_order_id)])

    def req_historical_data(self, contract, duration_str, bar_size_setting, what_to_show, end_date_time='', use_rth=0, format_date=2, keep_up_to_date=False, chart_options=[], timeout_load_factor=1):
        request_parameters = {"contract": contract, "durationStr": duration_str, "barSizeSetting": bar_size_setting,
                              "whatToShow": what_to_show, "endDateTime": end_date_time, "useRTH": use_rth,
                              "formatDate": format_date, "keepUpToDate": keep_up_to_date, "chartOptions": chart_options}
        points = int(duration_str.split(" ")[0])
        timeout_load_factor = min(timeout_load_factor, self.max_requests["reqHistoricalData"])
        if bar_size_setting == '1 min':
            factor = 1440
            timeout = factor
        elif bar_size_setting == '1 month':
            factor = 30
            timeout = factor
        else:
            factor = 10
            timeout = 30
        timeout += max(points * factor * timeout_load_factor//10000, int(sqrt(factor * timeout_load_factor)))    # heuristics

        connector_id, connector = self.broker_api_selector("reqHistoricalData")
        request_id = connector.next_req_id()

        request = Request(request_id, connector_id, "reqHistoricalData", request_parameters, timeout=timeout)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)

        self.request_counters["reqHistoricalData"].queue.put((connector_id, request_id))
        self.global_requests[(connector_id, request_id)] = request
        return request

    def _req_historical_data(self, request):
        connector = self.connectors[request.connector_id]
        connector.add_request(request)
        request.set_started()
        connector.broker_api.reqHistoricalData(request.request_id, **request.request_parameters)

    def cancel_historical_data(self, request, reason, no_lock=False):
        # print("cancel_historical_data", request.connector_id, request.request_id)
        if request.is_active():
            connector = self.connectors[request.connector_id]
            request.set_cancelled(reason)
            connector.broker_api.cancelHistoricalData(request.request_id)
            if not no_lock:
                with self.request_counters[request.request_type].lock:
                    self.request_counters[request.request_type].count -= 1

    def req_contract_details(self, contract):
        connector_id, connector = self.broker_api_selector("reqContractDetails")
        request_id = connector.next_req_id()
        request = Request(request_id, connector_id, "reqContractDetails", {"contract": contract}, timeout=STANDARD_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        connector.add_request(request)
        self.global_requests[(connector_id, request_id)] = request
        request.set_started()
        connector.broker_api.reqContractDetails(request.request_id, contract)
        return request

    def req_security_definition_option_parameters(self, underlying_symbol, fut_fop_exchange="", underlying_sec_type="STK", underlying_con_id=0):
        request_parameters = {"underlyingSymbol": underlying_symbol,
                              "futFopExchange": fut_fop_exchange,
                              "underlyingSecType": underlying_sec_type,
                              "underlyingConId": underlying_con_id}
        connector_id, connector = self.broker_api_selector("reqSecDefOptParams")
        request_id = connector.next_req_id()
        request = Request(request_id, connector_id, "reqSecDefOptParams", request_parameters, timeout=STANDARD_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        connector.add_request(request)
        self.global_requests[(connector_id, request_id)] = request
        request.set_started()
        connector.broker_api.reqSecDefOptParams(request_id, **request_parameters)
        return request

    def req_market_data(self, on_add_market_data, contract, generic_tick_list=None, snapshot=False, regulatory_snapshot=False,
                        market_data_options=None, is_busy=None):

        generic_tick_list = generic_tick_list or ''
        market_data_options = market_data_options or []

        request_parameters = {"contract": contract, "genericTickList": generic_tick_list, "snapshot": snapshot,
                              "regulatorySnapshot": regulatory_snapshot, "mktDataOptions": market_data_options}
        connector_id, connector = self.broker_api_selector("reqMktData")
        request_id = connector.next_req_id()

        request = MarketDataStreamRequest(request_id, connector_id, "reqMktData", request_parameters, timeout=MARKET_REQUEST_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error, on_get_data_postprocess=on_add_market_data, is_busy=is_busy)

        self.request_counters["reqMktData"].queue.put((connector_id, request_id))
        self.global_requests[(connector_id, request_id)] = request
        self.market_requests_by_symbol[request.request_symbol()] = request
        return request

    def _req_market_data(self, request):
        connector = self.connectors[request.connector_id]
        connector.add_request(request)
        request.set_started()
        connector.broker_api.reqMktData(request.request_id, **request.request_parameters)

    def cancel_market_data(self, request, reason=None, no_lock=False):
        if request.is_active():
            connector = self.connectors[request.connector_id]
            request.set_cancelled(reason)
            connector.broker_api.cancelMktData(request.request_id)
            if not no_lock:
                with self.request_counters[request.request_type].lock:
                    self.request_counters[request.request_type].count -= 1

    def req_positions(self): # one request per time only
        connector_id, connector = self.broker_api_selector("reqPositions")
        request = connector.get_special_request("reqPositions")
        if not request is None:
            return request
        request_id = connector.next_req_id()
        request = Request(request_id, connector_id, "reqPositions", None, timeout=POSITION_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        self.global_requests[(connector_id, request_id)] = request
        connector.add_request(request)
        request.set_started()
        connector.set_special_request("reqPositions", request)
        connector.broker_api.reqPositions()
        return request

    def req_cancel_positions(self, request, reason=None):
        connector = self.connectors[request.connector_id]
        request.set_cancelled(reason)
        connector.broker_api.cancelPositions(request.request_id)

    def req_positions_multi(self, account): # one request per time only
        request_parameters = {"account": account}
        connector_id, connector = self.broker_api_selector("reqPositionsMulti")
        request_id = connector.next_req_id()
        model_code = ""
        request = Request(request_id, connector_id, "reqPositionsMulti", request_parameters, timeout=POSITION_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        self.global_requests[(connector_id, request_id)] = request
        connector.add_request(request)
        request.set_started()
        connector.broker_api.reqPositionsMulti(request_id, account, model_code)
        return request

    def req_cancel_positions_multi(self, request, reason=None):
        if request.is_active():
            connector = self.connectors[request.connector_id]
            request.set_cancelled(reason)
            connector.broker_api.cancelPositionsMulti(request.request_id)

    def req_open_orders(self): # one request per time only
        connector_id, connector = self.broker_api_selector("reqOpenOrders")
        request = connector.get_special_request("reqOpenOrders")
        if not request is None:
            return request
        request_id = connector.next_req_id()
        request = Request(request_id, connector_id, "reqOpenOrders", None, timeout=POSITION_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        self.global_requests[(connector_id, request_id)] = request
        connector.add_request(request)
        request.set_started()
        connector.set_special_request("reqOpenOrders", request)
        connector.broker_api.reqOpenOrders()
        return request

    def req_place_order(self, order_post_process, contract, order, order_id=None):
        connector_id, connector = self.broker_api_selector("placeOrder")
        order_id = order_id or connector.next_req_id()
        request_parameters = {"order_id":order_id, "contract": contract, "order": order}
        request = OrderRequest(order_id, connector_id, "placeOrder", request_parameters)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error, on_get_data_postprocess=order_post_process)
        # request.set_handlers(on_finished=self.request_set_finished, on_error=None, on_get_data_postprocess=order_post_process)
#TODO check id cancel is OK
        self.global_requests[(connector_id, order_id)] = request
        connector.add_request(request)
        request.set_started()

        request_symbol = request.request_symbol()
        if order.orderType == "STP" or order.orderType == "TRAIL":
            if not request_symbol in self.stop_orders_by_symbol:
                self.stop_orders_by_symbol[request_symbol] = set()
            self.stop_orders_by_symbol[request_symbol].add(request)
        else:
            if order.action == "BUY":
                if not request_symbol in self.buy_orders_by_symbol:
                    self.buy_orders_by_symbol[request_symbol] = set()
                self.buy_orders_by_symbol[request_symbol].add(request)
            elif order.action == "SELL":
                if not request_symbol in self.sell_orders_by_symbol:
                    self.sell_orders_by_symbol[request_symbol] = set()
                self.sell_orders_by_symbol[request_symbol].add(request)

        connector.broker_api.placeOrder(order_id, contract, order)
        return request

    def req_cancel_order(self, request, reason=None):
        if request.is_active():
            connector = self.connectors[request.connector_id]
            request.set_cancelled(reason)
            connector.broker_api.cancelOrder(request.request_id, "")
            # order = request.request_parameters['order']
            # request_symbol = request.request_symbol()
            # if order.orderType == "STP" or order.orderType == "TRAIL":
            #     if not request_symbol in self.stop_orders_by_symbol:
            #         self.stop_orders_by_symbol[request_symbol].remove(request)
            # else:
            #     if order.action == "BUY":
            #         if not request_symbol in self.buy_orders_by_symbol:
            #             self.buy_orders_by_symbol[request_symbol].remove(request)
            #     elif order.action == "SELL":
            #         if not request_symbol in self.sell_orders_by_symbol:
            #             self.sell_orders_by_symbol[request_symbol].remove(request)

    def req_account_summary(self, tags):
        connector_id, connector = self.broker_api_selector("reqAccountSummary")
        request_id = connector.next_req_id()
        request = Request(request_id, connector_id, "reqAccountSummary", None, timeout=POSITION_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        self.global_requests[(connector_id, request_id)] = request
        connector.add_request(request)
        request.set_started()
        connector.broker_api.reqAccountSummary(request_id, groupName="All", tags=tags)
        return request

    def req_cancel_account_summary(self, request, reason=None):
        if request.is_active():
            connector = self.connectors[request.connector_id]
            request.set_cancelled(reason)
            connector.broker_api.cancelAccountSummary(request.request_id)

    def req_cancel_account_summary_nocheck(self, request, reason=None):
        connector = self.connectors[request.connector_id]
        request.set_cancelled(reason)
        connector.broker_api.cancelAccountSummary(request.request_id)

    def req_managed_accts(self, connector): # one request per time only
        connector_id = connector.client_id
        request = connector.get_special_request("reqManagedAccts")
        if not request is None:
            return request
        request_id = connector.next_req_id()
        request = Request(request_id, connector_id, "reqManagedAccts", None, timeout=MANAGED_ACCTS_TIMEOUT)
        request.set_handlers(on_finished=self.request_set_finished, on_error=self.request_set_cancelled_error)
        self.global_requests[(connector_id, request_id)] = request
        connector.add_request(request)
        request.set_started()
        connector.set_special_request("reqManagedAccts", request)
        connector.broker_api.reqManagedAccts()
        return request
