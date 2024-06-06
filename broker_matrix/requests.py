#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Encapsulated Request objects
reqHistoricalData
reqContractDetails
reqSecDefOptParams
reqMktData
reqPositions
reqPositionsMulti
reqOpenOrders
placeOrder
reqAccountSummary
reqManagedAccts
"""
from datetime import datetime, timedelta

MAX_REQUESTS = {"reqHistoricalData": 20, "reqMktData": 70}
REQUEST_CALLS = {"reqHistoricalData": "_req_historical_data", "reqMktData": "_req_market_data"}
REQUEST_CANCEL_CALLS = {"reqHistoricalData": "cancel_historical_data", "reqMktData": "cancel_market_data",
                        "reqContractDetails": None, "reqSecDefOptParams": None, "reqPositions": "req_cancel_positions",
                        "reqPositionsMulti": "req_cancel_positions_multi", "reqOpenOrders": None,
                        "reqAccountSummary": "req_cancel_account_summary", "reqManagedAccts": None}

class Request():
    def __init__(self, request_id, connector_id, request_type, request_parameters, timeout=None):
        self.request_id = request_id
        self.connector_id = connector_id
        self.request_type = request_type
        self.request_parameters = request_parameters
        self.timeout = timeout
        self.properties = {"TimedOut": False, "Error": False, "Started": False, "Finished": False, "Cancelled": False}  #volatile thread-safe properties
        self.errors = {}
        self.collected_data = [] # not thread-safe
        self.start_time = None
        self.end_time = None
        self.timeout_time = None
        self.on_get_data = None
        self.on_finished = None
        self.on_timeout = None
        self.on_error = None
        self.on_get_data_postprocess = None
        self.is_busy = None

    def set_handlers(self, on_get_data=None, on_finished=None, on_timeout=None, on_error=None, on_get_data_postprocess=None, is_busy=None):
        if on_get_data:
            self.on_get_data = on_get_data
        else:
            self.on_get_data = self.add_data
        if on_finished:
            self.on_finished = on_finished
        else:
            self.on_finished = self.set_finished
        if on_timeout:
            self.on_timeout = on_timeout
        if on_error:
            self.on_error = on_error
        if on_get_data_postprocess:
            self.on_get_data_postprocess = on_get_data_postprocess
        if is_busy:
            self.is_busy = is_busy

    def set_started(self):
        self.start_time = datetime.now()
        if not self.timeout is None:
            self.timeout_time = self.start_time + timedelta(seconds=self.timeout)
        else:
            self.timeout_time = None
        self.properties["Started"] = True

    def is_active(self):
        return self.properties["Started"] and not (self.properties["TimedOut"] or self.properties["Finished"] or self.properties["Cancelled"])

    def is_unfinished(self):
        return not self.properties['Finished'] and not self.properties['Cancelled']

    def add_data(self, data_piece, data_type=None):
        if not self.is_active():
            # print("got data for inactive request", data_piece, self.request_id, self.request_symbol(), self.properties)
            return
        self.collected_data.append(data_piece)

    def set_finished(self):
        self.end_time = datetime.now()
        self.properties["Finished"] = True

    def set_cancelled(self, reason=None):
        self.end_time = datetime.now()
        if not reason is None:
            self.properties["Reason"] = reason
        self.properties["Cancelled"] = True

    def is_finished(self):
        return self.properties["Finished"]

    def is_timed_out(self):
        return self.properties["TimedOut"]

    def get_data(self):
        return self.collected_data

    def request_sec_type(self):
        return self.request_parameters["contract"].secType

    def request_symbol(self):
        if not "contract" in self.request_parameters:
            return None
        if self.request_parameters["contract"].secType == "OPT":
            return (self.request_parameters["contract"].symbol, self.request_parameters["contract"].strike)
        return self.request_parameters["contract"].symbol

class MarketDataStreamRequest(Request):
    def __init__(self, request_id, connector_id, request_type, request_parameters, timeout=None):
        super().__init__(request_id, connector_id, request_type, request_parameters, timeout)
        self.collected_tick_sizes = []

    def add_data(self, data_piece, data_type=None):
        if not self.is_active():
            # print("got data for inactive request", data_piece, self.request_id, self.request_symbol(), self.properties)
            return
        if data_type == "price":
            self.collected_data.append(data_piece)
            self.on_get_data_postprocess(self, data_piece, data_type)
        elif data_type == "size":
            self.collected_tick_sizes.append(data_piece)
            self.on_get_data_postprocess(self, data_piece, data_type)

class OrderRequest(Request):
    def __init__(self, request_id, connector_id, request_type, request_parameters, timeout=None):
        super().__init__(request_id, connector_id, request_type, request_parameters, timeout)
        self.order_statuses = []
        self.execution_details = []
        self.commissions = []
        self.order_status = ""
        self.too_late_to_cancel = False
        self.child_order_id = None
        self.reason = ""

    def add_data(self, data_piece, data_type=None):
        if data_type == "order_status":
            self.order_statuses.append(data_piece)
        elif data_type == "execution_details":
            self.execution_details.append(data_piece)
        elif data_type == "commission":
            self.commissions.append(data_piece)
        self.on_get_data_postprocess(self, data_piece, data_type)

    def set_child_order_id(self, child_request):
        self.child_order_id = child_request.request_id
        
    def set_reason(self, reason):
        self.reason = reason
