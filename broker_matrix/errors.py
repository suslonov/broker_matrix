#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orders collection

"""

from ibapi.errors import CONNECT_FAIL, NOT_CONNECTED, BAD_LENGTH, BAD_MESSAGE, SOCKET_EXCEPTION, SSL_FAIL

def reconnect_errors():
    return [1100, 1300, CONNECT_FAIL.code(), NOT_CONNECTED.code(), BAD_LENGTH.code(),
                         BAD_MESSAGE.code(), SOCKET_EXCEPTION.code(), SSL_FAIL.code()]
    
def request_warnings():
    return [2100, 2101, 2102, 2103, 2104, 2158, 2105, 2106, 2107, 2108, 2109, 2110, 2137]
