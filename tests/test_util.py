import copy
import datetime
from decimal import Decimal
from typing import cast

import numpy as np
import pandas as pd
import pytest

from tax_loss.portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from tax_loss.trade import Side, Trade
from tax_loss.util import Schedule, repair_portfolio


def test_schedule():
    json_data = [
        {
            "clearingCycleEndTime": "0000",
            "tradingScheduleDate": "20000101",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
        },
        {
            "clearingCycleEndTime": "0000",
            "tradingScheduleDate": "20000102",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000103",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000104",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000105",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000106",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000107",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "0000",
            "tradingScheduleDate": "20220905",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
        },
    ]

    schedule = Schedule(json_data=json_data, exchange_tz="America/New_York")
    ts = pd.Timestamp("20220906 9:00").tz_localize("America/Chicago")  # regular day, regular time
    assert schedule.is_open(ts)
    ts = pd.Timestamp("20220906 17:00").tz_localize("America/Chicago")  # regular day, after market hours
    assert not schedule.is_open(ts)
    ts = pd.Timestamp("20220905 10:00").tz_localize("America/Chicago")  # holiday, regular time
    assert not schedule.is_open(ts)


@pytest.fixture
def portfolio():
    pf = Portfolio()
    pf.cash = 25000.0
    for i, ticker in enumerate(["ABC", "XYZ"]):
        pf.ticker_to_cost_basis[ticker] = CostBasisInfo(ticker=ticker, tax_lots=[TaxLot(shares=10, price=i + 1)])
        pf.ticker_to_market_price[ticker] = MarketPrice(price=i * 10.0, last_updated=datetime.datetime.now())
    return pf


def test_repair_portfolio(portfolio: Portfolio):
    stale_pf = portfolio
    new_pf = copy.deepcopy(portfolio)
    trades = [
        Trade("ABC", Decimal(10), Decimal(1), Side.BUY, exchange_ts=pd.Timestamp("2022 09 12 10:00")),
        Trade("XYZ", Decimal(10), Decimal(2), Side.BUY, exchange_ts=pd.Timestamp("2022 09 12 10:01")),
        Trade("ABC", Decimal(5), Decimal(12), Side.BUY, exchange_ts=pd.Timestamp("2022 09 12 10:02")),
        Trade("ABC", Decimal(5), Decimal(12), Side.BUY, exchange_ts=pd.Timestamp("2022 09 12 10:03")),
    ]

    new_pf.buy(ticker="ABC", shares=Decimal("5"), price=12.0, fee=0.0)
    new_pf.buy(ticker="ABC", shares=Decimal("5"), price=12.0, fee=0.0)

    repaired_pf = repair_portfolio(stale_portfolio=stale_pf, target_portfolio=new_pf, trades=trades)
    repaired_pf = cast(Portfolio, repaired_pf)
    assert repaired_pf.positions == new_pf.positions
    assert np.isclose(repaired_pf.cash, new_pf.cash)

    new_pf.buy(ticker="XYZ", shares=Decimal("10"), price=1.0, fee=0.0)
    repaired_pf = repair_portfolio(stale_portfolio=stale_pf, target_portfolio=new_pf, trades=trades)
    assert repaired_pf is None
