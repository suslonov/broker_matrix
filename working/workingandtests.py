#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test and buffer
"""

import time
import pandas as pd

from ibapi.contract import Contract
from connection_matrix import ConnectionMatrix

SYMBOLS = "AAPL AMZN BA BABA BIDU CGC CRIS DIS DRN DRV EDC EDZ FAS FAZ FB FNGD FNGU GLD GM GOVX HEPA IEF INO JDST JNUG LABD LABU MNKD MRNA MSFT NET NFLX NNOX NVAX NVDA QQQ SHY SLV SPXS SPY TECL TECS TLT TMV TNA TSLA TZA VERU VTVT XLI XLU".split(" ")
# SYMBOLS = ['AAPL', 'AMZN', 'BA', 'BABA', 'BIDU', 'CGC', 'CRIS', 'DIS', 'FB', 'GLD', 'GM', 'GOVX', 'HEPA', 'INO',
           # 'MNKD', 'MRNA', 'MSFT', 'NET', 'NFLX', 'NNOX', 'NVAX', 'NVDA', 'SLV', 'SPY', 'TSLA', 'VERU', 'VTVT']

def contract(symbol, secType='STK', currency='USD', exchange='ISLAND', primaryExchange='ISLAND'):
    c = Contract()
    c.symbol = symbol
    c.secType = secType 
    c.currency = currency
    c.exchange = exchange
    c.primaryExchange = primaryExchange
    return c

def print_requests_times(requests):
    max_symbol_time = 0; min_time = 0; max_time = 0; count = 0
    for symbol in requests:
        try:
            print(symbol, requests[symbol].properties["Cancelled"],requests[symbol].timeout,requests[symbol].start_time, requests[symbol].end_time, requests[symbol].end_time-requests[symbol].start_time)
            if max_symbol_time == 0:
                max_symbol_time = requests[symbol].end_time-requests[symbol].start_time
            else:
                max_symbol_time = max([max_symbol_time, requests[symbol].end_time-requests[symbol].start_time])
            if min_time == 0:
                min_time = requests[symbol].start_time
            else:
                min_time = min([min_time, requests[symbol].start_time])
            if max_time == 0:
                max_time = requests[symbol].end_time
            else:
                max_time = max([max_time, requests[symbol].end_time])
        except:
            print(symbol, requests[symbol].properties, requests[symbol].timeout)
            count += 1
    print(max_symbol_time, min_time, max_time, max_time-min_time, count)

def count_requests_times(requests):
    max_symbol_time = 0; min_time = 0; max_time = 0; count = 0
    for symbol in requests:
        try:
            if max_symbol_time == 0:
                max_symbol_time = requests[symbol].end_time-requests[symbol].start_time
            else:
                max_symbol_time = max([max_symbol_time, requests[symbol].end_time-requests[symbol].start_time])
            if min_time == 0:
                min_time = requests[symbol].start_time
            else:
                min_time = min([min_time, requests[symbol].start_time])
            if max_time == 0:
                max_time = requests[symbol].end_time
            else:
                max_time = max([max_time, requests[symbol].end_time])
        except:
            count += 1
    return max_symbol_time, min_time, max_time, max_time-min_time, count

matrix = ConnectionMatrix()
matrix.start()
matrix.create_connection(["reqHistoricalData", "reqContractDetails", "reqSecDefOptParams"])

requests = {}; collected_data = {}
for symbol in SYMBOLS:
    c = contract(symbol)
    r = matrix.req_historical_data(c, durationStr='10 D', barSizeSetting='1 min', whatToShow='TRADES', timeout_load_factor=len(SYMBOLS))
    requests[symbol] = r

print_requests_times(requests)

matrix.stop()

for symbol in requests:
    print(requests[symbol].errors)

for i in range(10):
    print_requests_times(requests)
    time.sleep(60)


for symbol in requests:
    if requests[symbol].is_finished():
        collected_data[symbol] = matrix.get_historical_data(requests[symbol].connector_id, requests[symbol].request_id)

for symbol in collected_data:
    print(symbol, len(collected_data[symbol]))




matrix = ConnectionMatrix()
matrix.start()
matrix.create_connection(["reqHistoricalData", "reqContractDetails", "reqSecDefOptParams"])

requests = {}
c = contract("AAPL")
r = matrix.req_contract_details(c)
print(r.properties)

matrix.stop()






    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice) 

    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action, order.orderType, order.totalQuantity, orderState.status)

    def execDetails(self, reqId, contract, execution):
        print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)
        
    def updateMktDepth(self, reqId, position, operation, side, price, size):
        print(f'reqId: {reqId} position: {position} operation: {operation} side: {side} price: {price}, size: {size}')

    def mktDepthExchanges(self,	depthMktDataDescriptions):
        print(depthMktDataDescriptions)
        
    def position(self, account, contract, position, avgCost):
        print(account, contract, position, avgCost)
        
    def positionEnd(self):
        print("positionEnd")
        
    def positionMulti(self, reqId, account, model, contract, position, avgCost):
        self.account_position_data[reqId]["data"].loc[len(self.account_position_data[reqId]["data"])] = [account, model, contract, position, avgCost]

    def positionMultiEnd(self, reqId):
        self.account_position_data[reqId]["finished"] = True

    def accountSummary(self, reqId, account, tag, value, currency):
        self.account_summary_data[reqId]["data"].loc[len(self.account_summary_data[reqId]["data"])] = [account, tag, value, currency]
        
    def accountSummaryEnd(self, reqId):
        self.account_summary_data[reqId]["finished"] = True




    def reqMktData(self, contract, reqId = None, genericTickList = '', snapshot=False, regulatorySnaphsot=False, mktDataOptions=[]):
        if not reqId:
            reqId = self.next_reqId()
        self.app.reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnaphsot, mktDataOptions)
# add processing        
        return reqId

    def cancelMktData(self, reqId):
        self.app.cancelMktData(reqId)



    def stock_contract(self, symbol, secType='STK', exchange='SMART', currency='USD'):
        c = Contract()
        c.symbol = symbol
        c.secType = secType
        c.exchange = exchange
        c.currency = currency
        return c
    
    def reqIds(self):
        self.app.reqIds(0)

    def submit_market_order(self, contract, action, account, qty, transmit=True):
        order = Order()
        order.action = action
        order.account = account
        order.totalQuantity = qty
        order.orderType = "MKT"
        order.transmit = transmit
        order_id = self.app.nextorderId
        self.app.placeOrder(order_id, contract, order)
        self.app.nextorderId += 1
        return order_id
    
    def submit_limit_order(self, contract, action, account, qty, lmt_price, transmit=True):
        order = Order()
        order.action = action
        order.account = account
        order.totalQuantity = qty
        order.orderType = "LMT"
        order.lmtPrice = lmt_price
        order.transmit = transmit
        order_id = self.app.nextorderId
        self.app.placeOrder(order_id, contract, order)
        self.app.nextorderId += 1
        return order_id
    
    def cancel_order(self, order_id):
        self.app.cancelOrder(order_id)

    def reqAllOpenOrders(self):
        self.app.reqAllOpenOrders()

    def reqPositions(self):
        self.app.reqPositions()

    def reqAccountSummary(self, reqId=None, group="All", tags="BuyingPower"):
        if not reqId:
            reqId = self.next_reqId()
        data_to_load = pd.DataFrame(columns=['account', 'tag', 'value', 'currency'])
        self.app.account_summary_data[reqId] = {"data": data_to_load, "finished": False}
        self.app.reqAccountSummary(reqId, group, tags)
        return reqId

    def account_summary_finished(self, reqId):
        return self.app.account_summary_data[reqId]["finished"]

    def get_account_summary(self, reqId):
        return self.app.account_summary_data[reqId]["data"]
    
    def cancelAccountSummary(self, reqId):
        self.app.cancelAccountSummary(reqId)

    def reqPositionsMulti(self, reqId=None, account="", model_code=""):
        if not reqId:
            reqId = self.next_reqId()
        data_to_load = pd.DataFrame(columns=['account', 'model', 'contract', 'position', 'avgCost'])
        self.app.account_position_data[reqId] = {"data": data_to_load, "finished": False}
        self.app.reqPositionsMulti(reqId, account, model_code)
        return reqId

    def position_multi_finished(self, reqId):
        return self.app.account_position_data[reqId]["finished"]

    def get_position_multi_summary(self, reqId):
        return self.app.account_position_data[reqId]["data"]
    
    def cancelPositionsMulti(self, reqId):
        self.app.cancelPositionsMulti(reqId)



