#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from broker_matrix import IBLayer, option_contract

def make_option_symbol_string(symbol, strike, direction, expiration):
    integer_fractional = strike.split('.')
    if len(integer_fractional) > 1:
        fractional = f'{int(integer_fractional[1]):d}'
    else:
        fractional = ""
    if len(fractional) < 3:
        fractional = fractional + '0'*(3-len(fractional))
    strike_string = f'{int(integer_fractional[0]):05d}' + fractional
    return symbol + expiration + direction + strike_string

SYMBOLS = ["AAPL", "SPY"]

ib = IBLayer(account="DU2096488",
             currency="USD",
             client_id=987654,
             remote="aws_ib", # it points to 10.0.1.153 , you can specify host= instead of remote=
#             pls look at lib2.server_definitions
             port=4002, # 
             request_type_groups=['Historical'])

ib.start()

bars_number = 600
collected_data = ib.retrieve_ib_historical_data(SYMBOLS,
                                                duration_str=str(bars_number)+' S',
                                                bar_size_setting='1 min',
                                                what_to_show='TRADES')
# https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration

options_for_symbols = {symbol: ib.retrieve_option_parameters(symbol) for symbol in SYMBOLS}

options_for_symbols["SPY"]["expirations"] #all awailable expirations
options_for_symbols["SPY"]["all_strikes"] #all awailable strikes, sometimes it includes non-existing strikes (fractional, for example)

#option contracts lattice example
symbol = "SPY"
requests = {}
bars_number = 60
for e in options_for_symbols["SPY"]["expirations"][5:8]:
    for s in [445, 448, 451, 454]:
        option_symbol_string = make_option_symbol_string(symbol, str(s), "C", e)
        requests[option_symbol_string] = (option_symbol_string, bars_number, option_contract(symbol, str(s),
                                                   e, options_for_symbols[symbol]["multiplier"], "C"))
        option_symbol_string = make_option_symbol_string(symbol, str(s), "P", e)
        requests[option_symbol_string] = (option_symbol_string, bars_number, option_contract(symbol, str(s),
                                                   e, options_for_symbols[symbol]["multiplier"], "P"))

#can take some time
collected_option_data = ib.retrieve_ib_historical_data_general(requests,
                                                            duration_str_suffix=" S",
                                                            bar_size_setting='1 secs',
                                                            what_to_show='TRADES',
                                                            options=True)

ib.stop()
