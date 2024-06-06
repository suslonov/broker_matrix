#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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


for i in [7, 8, 10, 12, 15, 20, 25, 30]:
    matrix = ConnectionMatrix({"reqHistoricalData": i})
    matrix.start()
    matrix.create_connection(["reqHistoricalData", ])
    # matrix.create_connection(["reqHistoricalData", ])
    
    requests = {}; collected_data = {}
    for symbol in SYMBOLS:
        c = contract(symbol)
        r = matrix.req_historical_data(c, durationStr='10 D', barSizeSetting='1 min', whatToShow='TRADES', timeout_load_factor=len(SYMBOLS))
        requests[symbol] = r
    
    for ii in range(100):
        max_symbol_time, min_time, max_time, total, count = count_requests_times(requests)
        if count == 0:
            print(i, total, total/len(SYMBOLS), max_symbol_time)
            break
        print(i, ii, "waiting")
        time.sleep(60)
    else:
        print("global timeout")
    
    matrix.close_all_connections()
    time.sleep(10)

matrix.stop()

