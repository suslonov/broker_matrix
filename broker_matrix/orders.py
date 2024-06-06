#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orders collection

"""

from ibapi.order import Order

def market_order(action, account, total_quantity, transmit=True):
    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.account = account
    order.totalQuantity = total_quantity
    order.transmit = transmit
    return order

def limit_order(action, account, lmt_price, total_quantity, tif=None, transmit=True):
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.account = account
    order.lmtPrice = lmt_price
    order.totalQuantity = total_quantity
    order.transmit = transmit
    if tif:
        order.tif = tif
    return order

def stop_order(action, account, aux_price, total_quantity, parent_order_id, trigger_method=0, transmit=True):
    order = Order()
    order.action = action
    order.orderType = "STP"
    order.triggerMethod = trigger_method
    order.account = account
    order.auxPrice = aux_price
    order.totalQuantity = total_quantity
    order.transmit = transmit
    order.parentId = parent_order_id
    return order

"""
TriggerMethod
0 - The default value. The "double bid/ask" function will be used for orders for OTC stocks and US options. All other orders will used the "last" function.
1 - use "double bid/ask" function, where stop orders are triggered based on two consecutive bid or ask prices.
2 - "last" function, where stop orders are triggered based on the last price.
3 double last function.
4 bid/ask function.
7 last or bid/ask function.
8 mid-point function.
"""
def stop_trailing_order(action, account, trailing_percent=None, trail_stop_price=None, trailing_amount=None, total_quantity=None, parent_order_id=None, trigger_method=8, transmit=True):
    order = Order()
    order.action = action
    order.orderType = "TRAIL"
    order.triggerMethod = trigger_method
    order.account = account
    if not trailing_percent is None:
        order.trailingPercent = trailing_percent
    else:
        order.auxPrice = trailing_amount
    if not trail_stop_price is None:
        order.trailStopPrice = trail_stop_price
    order.totalQuantity = total_quantity
    order.transmit = transmit
    order.parentId = parent_order_id
    return order
