#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from broker_matrix import IBLayer, stocks_contract


symbols = "AAPL AMZN BA BABA BIDU CGC CRIS DIS DRN DRV EDC EDZ FAS FAZ FB FNGD FNGU GLD GM GOVX HEPA IEF INO JDST JNUG LABD LABU MNKD MRNA MSFT NET NFLX NNOX NVAX NVDA QQQ SHY SLV SPXS SPY TECL TECS TLT TMV TNA TSLA TZA VERU VTVT XLI XLU".split(" ")
symbols = "AAPL AMZN".split(" ")
symbol="AAPL"

ib = IBLayer()
ib.start()

contract = stocks_contract("AMZN")

contract_details = ib.retrieve_contract_details(contract)

option_symbol = ib.retrieve_option_parameters(symbol="AMZN")

the_data = ib.retrieve_ib_historical_data(["AAPL", "AMZN"], "5 D", "1 day")

the_data_min = ib.retrieve_ib_historical_data(["AAPL", "AMZN"], "5 D", "1 min")

ib.stop()
