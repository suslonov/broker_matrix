#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Common brocker level for IB

"""
import time
from datetime import datetime
import pytz
from tzlocal import get_localzone_name
import pandas as pd

from ibapi.contract import Contract
from ibapi.order import Order
from .connection_matrix import ConnectionMatrix
from .orders import market_order, limit_order, stop_order, stop_trailing_order
from .contracts import stocks_contract, option_contract

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
LOCAL_TIMEZONE = pytz.timezone(get_localzone_name())
REF_EXCHANGE = 'CBOE'

CONTRACT_DETAILS_CHECK_TIMEOUT = 20
CONTRACT_DETAILS_CHECK_SLEEP = 0.01
CHAIN_TIMEOUT = 120
CHAIN_SLEEP = 0.1

def count_unfinished_requests(requests):
    count = 0
    for symbol in requests:
        if requests[symbol].is_unfinished():
            count += 1
    return count

class IBLayer(ConnectionMatrix):
    def __init__(self, account=None, currency=None, client_id=None, remote=None, host=None, port=None, request_type_groups=None):
        super().__init__(client_id=client_id)
        self.account = account
        self.currency = currency
        self.host = host
        self.port = port
        self.remote = remote
        self.request_type_groups = request_type_groups if not request_type_groups is None else ['Historical']

    def start(self):
        super().start()
        if 'Historical' in self.request_type_groups:
            self.create_connection(request_types=['reqHistoricalData',
                                                  'reqContractDetails',
                                                  'reqSecDefOptParams',
                                                  "reqPositions",
                                                  "reqPositionsMulti",
                                                  "reqOpenOrders",
                                                  "reqAccountSummary",
                                                  "placeOrder"], remote=self.remote, host=self.host, port=self.port)
        if "Market" in self.request_type_groups:
            self.create_connection(request_types=["reqMktData"], remote=self.remote, host=self.host, port=self.port)
        # if "Order" in self.request_type_groups:
        #     self.create_connection(request_types=["createOrder"], host=self.host, port=self.port)

    # def stop(self):

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def get_historical_data(self, connector_id, request_id, incomplete=False):
        request = self.global_requests[(connector_id, request_id)]
        if not request.properties['Finished'] and not incomplete:
            return None
        return pd.DataFrame.from_records(request.get_data(), columns=['date', 'open', 'high', 'low', 'close', 'volume'], index='date')

    def retrieve_ib_historical_data(self, symbols, duration_str, bar_size_setting, what_to_show='TRADES', localize=True):
        requests = {}; collected_data = {}
        for symbol in symbols:
            requests[symbol] = self.req_historical_data(stocks_contract(symbol),
                                                        duration_str=duration_str,
                                                        bar_size_setting=bar_size_setting,
                                                        what_to_show=what_to_show,
                                                        timeout_load_factor=len(symbols))

        while count_unfinished_requests(requests):
            time.sleep(1)

        for symbol in requests:
            historical_data = self.get_historical_data(requests[symbol].connector_id, requests[symbol].request_id)
            if historical_data is None or len(historical_data) == 0:
                collected_data[symbol] = None
                continue

            if localize:
                historical_data.index = historical_data.apply(lambda row: pd.Timestamp(LOCAL_TIMEZONE.localize(row.name).astimezone(EASTERN)), axis=1)
            historical_data.index.name = 'Date'
            collected_data[symbol] = historical_data

        return collected_data

    def retrieve_ib_volatility(self, symbols, duration, bar_size_setting):
        vol = self.retrieve_ib_historical_data(symbols, duration, bar_size_setting, what_to_show='HISTORICAL_VOLATILITY')
        return {symbol: vol[symbol]['close'][-1] for symbol in vol if (not vol[symbol] is None and not vol[symbol]['close'] is None and len(vol[symbol]['close']) > 0)}

    def retrieve_ib_historical_data_general(self, symbol_requests, duration_str_suffix, bar_size_setting, what_to_show='TRADES', localize=True, options=False, end_date_time=''):
        if options:
            column = 1
        else:
            column = 0
        requests = {}; collected_data = {}
        for symbol in symbol_requests:
            contract = symbol_requests[symbol][column + 1]
            duration_str = str(symbol_requests[symbol][column]) + duration_str_suffix
            requests[symbol] = self.req_historical_data(contract=contract,
                                                        duration_str=duration_str,
                                                        bar_size_setting=bar_size_setting,
                                                        what_to_show=what_to_show,
                                                        end_date_time=end_date_time)

        while count_unfinished_requests(requests):
            #TODO check if the connection is dead or stalled, go out, process available and return result code
            time.sleep(1)

        for symbol in requests:
            if not requests[symbol].properties['Finished']:
                collected_data[symbol] = None
                continue
            historical_data = self.get_historical_data(requests[symbol].connector_id, requests[symbol].request_id)

            if historical_data is None or len(historical_data) == 0:
                collected_data[symbol] = None
                continue

            if localize:
                historical_data.index = historical_data.apply(lambda row: pd.Timestamp(LOCAL_TIMEZONE.localize(row.name).astimezone(EASTERN)), axis=1)
            historical_data.index.name = 'Date'
            collected_data[symbol] = historical_data

        return collected_data

    def retrieve_contract_details(self, contract):
        request = self.req_contract_details(contract)
        count = CONTRACT_DETAILS_CHECK_TIMEOUT
        while count > 0:
            if request.properties['Finished']:
                return request.get_data()[0]
            if request.properties['Cancelled']:
                return None
            time.sleep(CONTRACT_DETAILS_CHECK_SLEEP)
            count -= CONTRACT_DETAILS_CHECK_SLEEP
        return None

    def contract_details_check(self, contract):
        request = self.req_contract_details(contract)
        count = CONTRACT_DETAILS_CHECK_TIMEOUT
        while count > 0:
            if request.properties['Finished']:
                return True
            if request.properties['Cancelled']:
                return False
            time.sleep(CONTRACT_DETAILS_CHECK_SLEEP)
            count -= CONTRACT_DETAILS_CHECK_SLEEP
        return False

    def retrieve_option_parameters(self, symbol):
        option_symbol = {}
        contract_details = self.retrieve_contract_details(stocks_contract(symbol))
        if contract_details is None:
            return None

        underlying_con_id = contract_details.contract.conId
        min_tick = contract_details.minTick
        request = self.req_security_definition_option_parameters(symbol, underlying_con_id=underlying_con_id)

        count = CHAIN_TIMEOUT
        while count > 0:
            if request.properties['Finished']:
                break
            if request.properties['Cancelled']:
                return None
            time.sleep(CHAIN_SLEEP)
            count -= CHAIN_SLEEP
        else:
            return None

        chain = request.get_data()
        for exchange_option in chain:
            if exchange_option['exchange'] == REF_EXCHANGE:
                all_strikes = list(exchange_option['strikes'])
                expirations = list(exchange_option['expirations'])
                multiplier = int(exchange_option['multiplier'])
                break
        else:
            # TODO do smth because we don't find our reference exchange
            return None

        option_symbol['underlying_con_id'] = underlying_con_id
        option_symbol['min_tick'] = min_tick
        option_symbol['multiplier'] = multiplier

        expirations.sort()
        closest_expiration = expirations[0]
        closest_expiration_date = datetime(int(closest_expiration[:4]), int(closest_expiration[4:6]), int(closest_expiration[-2:]))
        if closest_expiration_date.date() == datetime.now(tz=JERUSALEM).astimezone(EASTERN).date():
            closest_expiration = expirations[1]
            closest_expiration_date = datetime(int(closest_expiration[:4]), int(closest_expiration[4:6]), int(closest_expiration[-2:]))
# TODO add hour
        option_symbol['closest_expiration'] = closest_expiration
        option_symbol['closest_expiration_date'] = closest_expiration_date
        option_symbol['expirations'] = expirations

        all_strikes.sort()
        option_symbol['all_strikes'] = all_strikes
        option_symbol['strikes'] = []
        option_symbol['contracts'] = {}

        return option_symbol

    # def request_option_contract_market_data(self, contract):
    #     request = self.ib_client.req_market_data(some_listener_for_options, contract)
    #     return request

    def is_request_busy_dumb(self, request):
        return True

    def request_assets(self, assets_to_request, listener_for_assets):
        already_requested = []
        for request in self.active_requests():
            if request.request_type != "reqMktData" or request.request_sec_type() != "STK":
                continue
            request_symbol = request.request_symbol()
            if not request_symbol in assets_to_request:
                self.cancel_market_data(request, "Unselected")
            else:
                already_requested.append(request_symbol)

        for symbol in assets_to_request:
            if not symbol in already_requested:
                asset_contract = stocks_contract(symbol)
                self.req_market_data(listener_for_assets, asset_contract, is_busy=self.is_request_busy_dumb)

    def is_it_worth_to_cancel_request(self, request):
        return ((request.is_busy is None or not request.is_busy(request))
                and (self.request_counters[request.request_type].count >= self.max_requests[request.request_type] - 1
                     or self.request_counters[request.request_type].queue.qsize() > 0))

    def request_options(self, options_to_request, all_options, listener_for_options, is_busy):
        # print(options_to_request)
        already_requested = []
        for request in self.active_requests():
            if request.request_type != "reqMktData" or request.request_sec_type() != "OPT":
                continue
            request_symbol = request.request_symbol()
            # print(request_symbol)
            if not request_symbol in options_to_request:
                if self.is_it_worth_to_cancel_request(request):
                    self.cancel_market_data(request, "Unselected")
            else:
                already_requested.append(request_symbol)

        for symbol, strike in options_to_request:
            if (symbol, strike) not in already_requested and not self.check_request_in_the_queue("reqMktData", (symbol, strike)):
                contract = all_options[symbol]["contracts"][strike]
                self.req_market_data(listener_for_options, contract, is_busy=is_busy)

    def cancel_all_requests(self):
        for request in self.global_requests.values():
            if not request.is_active():
                continue
            if request.request_type == "reqHistoricalData":
                self.cancel_historical_data(request, "Finish")
            elif request.request_type == "reqMktData":
                self.cancel_market_data(request, "Finish")

    def get_current_price(self, symbol):
        if not symbol in self.market_requests_by_symbol:
            return None
        request = self.market_requests_by_symbol[symbol]
        return pd.DataFrame.from_records(request.get_data(), columns=['datetime', 'type', 'price'], index='datetime')

    def get_option_current_price(self, symbol):
        return self.get_current_price(symbol)

    def retrieve_positions(self):
        request = self.req_positions()
        while request.is_unfinished():
            time.sleep(1)
        if request.properties['Finished']:
            columns = ['account', 'symbol', 'strike', 'secType', 'lastTradeDateOrContractMonth', 'position', 'avgCost', 'contract']
            return pd.DataFrame.from_records(request.get_data(), columns=columns)
        return None

    def retrieve_positions_multi(self, account=None):
        if account is None:
            account = self.account
        request = self.req_positions_multi(account)
        while request.is_unfinished():
            time.sleep(1)
        if request.properties['Finished']:
            columns = ['account', 'symbol', 'strike', 'secType', 'lastTradeDateOrContractMonth', 'position', 'avgCost', 'contract', 'modelCode']
            return pd.DataFrame.from_records(request.get_data(), columns=columns)
        return None

    def buy_call_with_trailing_stop(self, order_post_process, symbol, strike, contract, price, quantity, limit_price=None, stop_loss=None, stop_loss_initial=None):
        if limit_price is None:
            parent_order = market_order(action="BUY", account=self.account,
                                        total_quantity=quantity,
                                        transmit=False)
        else:
            parent_order = limit_order(action="BUY", account=self.account,
                                       lmt_price=limit_price,
                                       total_quantity=quantity,
                                       # tif="FOK",
                                       transmit=False)
        request_parent = self.req_place_order(order_post_process, contract, parent_order)

        trailing_amount = round(price * stop_loss - 0.005, 2)
        trail_stop_price = round(price * (1 - stop_loss_initial) - 0.005, 2)
        stop_loss_order = stop_trailing_order(action="SELL", account=self.account,
                                              trail_stop_price=trail_stop_price,
                                              trailing_amount=trailing_amount,
                                              total_quantity=quantity, parent_order_id=request_parent.request_id)
        # stop_loss_order = stop_trailing_order(action="SELL", account=self.account,
        #                                       trailing_percent=stop_loss,
        #                                       trail_stop_price=trail_stop_price,
        #                                       total_quantity=quantity, parent_order_id=request_parent.request_id)
        request_stop = self.req_place_order(order_post_process, contract, stop_loss_order)
        request_parent.set_child_order_id(request_stop)

        print("Buy with trailing stop", request_parent.request_id, (symbol, strike), quantity, (price, limit_price, trail_stop_price))
        return request_parent, request_stop

    def buy_call_with_stop(self, order_post_process, symbol, strike, contract, price, quantity, limit_price, stop_loss):
        # stop_loss = parameters['STOP_LOSS']
        parent_order = limit_order(action="BUY", account=self.account,
                                   lmt_price=limit_price,
                                   total_quantity=quantity,
                                   # tif="FOK",
                                   transmit=False)
        request_parent = self.req_place_order(order_post_process, contract, parent_order)

        stop_loss_order = stop_order(action="SELL", account=self.account,
                                     aux_price=round(price * (1 - stop_loss) + 0.005, 2),
                                     total_quantity=quantity, parent_order_id=request_parent.request_id)
        request_stop = self.req_place_order(order_post_process, contract, stop_loss_order)
        request_parent.set_child_order_id(request_stop)

        print("Buy with hard stop", request_parent.request_id, (symbol, strike), quantity, (price, limit_price, price * (1 - stop_loss)))
        return request_parent, request_stop

    def buy_call_limit(self, order_post_process, symbol, strike, contract, price, quantity, limit_price=None):
        if limit_price is None:
            order = market_order(action="BUY", account=self.account,
                                 total_quantity=quantity)
        else:
            order = limit_order(action="BUY", account=self.account,
                                lmt_price=limit_price,
                                total_quantity=quantity)
        request_order = self.req_place_order(order_post_process, contract, order)

        print("Buy with limit no stops", request_order.request_id, (symbol, strike), quantity, (price, limit_price))
        return request_order

    def sell_call_contract(self, order_post_process, contract, quantity):
        order = market_order(action="SELL", account=self.account, total_quantity=quantity)
        request = self.req_place_order(order_post_process, contract, order)
        print("Sell", request.request_id, request.request_symbol(), quantity)
        return request

    def buy_call_contract(self, order_post_process, contract, quantity):
        order = market_order(action="BUY", account=self.account, total_quantity=quantity)
        request = self.req_place_order(order_post_process, contract, order)
        print("Buy", request.request_id, request.request_symbol(), quantity)
        return request

    def cancel_all_orders(self):
        for request in self.global_requests.values():
            if request.request_type == "placeOrder":
                if not request.is_active():
                    continue
                self.req_cancel_order(request, "Finish")

# req = ib.buy_call_contract(renew_mill.order_post_process, my_context["options"]['AAPL']["contracts"][150], 1)
# ib.req_cancel_order(req)

    def retrieve_accounts_summary(self):
        request = self.req_account_summary(tags="TotalCashValue, SettledCash, AccruedCash, BuyingPower, EquityWithLoanValue, PreviousEquityWithLoanValue, GrossPositionValue")
        while request.is_unfinished():
            time.sleep(1)
        self.req_cancel_account_summary_nocheck(request) # not always correctly finshed ?
        if request.properties['Finished']:
            columns = ['account', 'tag', 'value', 'currency']
            return pd.DataFrame.from_records(request.get_data(), columns=columns)
        return None

    def get_account_cash_total_value(self):
        accounts_summary = self.retrieve_accounts_summary()
        return float(accounts_summary.loc[(accounts_summary.account == self.account)
                                          & (accounts_summary.currency == self.currency)
                                          & (accounts_summary.tag == "TotalCashValue")].iloc[0]["value"])
