#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orders collection

"""

from ibapi.contract import Contract

def stocks_contract(symbol, sec_type='STK', currency='USD', exchange='SMART', primary_exchange='ISLAND'):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    contract.primaryExchange = primary_exchange
    return contract

def option_contract(symbol, strike, expiration_str, multiplier, direction="C"):
    opt_contract = Contract()
    opt_contract.symbol = symbol
    opt_contract.secType = "OPT"
    opt_contract.exchange = "SMART"
    opt_contract.currency = "USD"
    opt_contract.lastTradeDateOrContractMonth = expiration_str
    opt_contract.strike = strike
    opt_contract.right = direction
    opt_contract.multiplier = multiplier
    return opt_contract
