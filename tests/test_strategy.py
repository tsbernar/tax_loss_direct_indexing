import datetime
from decimal import Decimal

import mock
import munch
import pandas as pd
import pytest
from freezegun import freeze_time

from tax_loss.portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from tax_loss.strategy import DirectIndexTaxLossStrategy
from tax_loss.trade import Side


@pytest.fixture
def config():
    config = munch.munchify(
        {
            "ticker_blacklist_extra": [],
            "price_data_file": "filename",
            "portfolio_file": "filename",
            "ticker_blacklist_file": "filename",
            "max_stocks": 100,
            "index_weight_file": "filename",
            "optimizer": {
                "lookback_days": 10,
                "tax_coefficient": 0.4,
                "max_deviation_from_true_weight": 0.05,
                "cash_constraint": 0.9,
                "tracking_error_func": "least_squared",
                "max_total_deviation": 0.5,
            },
            "gateway": {},
        }
    )
    return config


def test_blacklisting(config):
    json_data = '{"AAPL" : "2022-08-15", "GME" : null, "VTI": "2022-08-16", "ABC" : "2022-08-17"}'
    mock_self = mock.Mock()
    with freeze_time("2022-08-16 10:00"), mock.patch("builtins.open", mock.mock_open(read_data=json_data)):
        ticker_blacklist = DirectIndexTaxLossStrategy._load_ticker_blacklist(mock_self, "dummy_filename", config)
        assert ticker_blacklist == {"GME": None, "ABC": datetime.date(2022, 8, 17)}


def test_plan_transactions():
    mock_self = mock.Mock()
    mock_self.ticker_blacklist = {"ABC": None}
    current = Portfolio(cash=200)
    desired = Portfolio(
        cash=0,
        ticker_to_cost_basis={"XYZ": CostBasisInfo("XYZ", [TaxLot(shares=10, price=20)])},
        ticker_to_market_price={"XYZ": MarketPrice(20, last_updated=datetime.datetime.now())},
    )
    transactions = DirectIndexTaxLossStrategy._plan_transactions(
        mock_self, desired_portfolio=desired, current_portfolio=current
    )
    assert transactions[0].symbol == "XYZ"
    assert transactions[0].side == Side.BUY
    assert transactions[0].qty == Decimal("10")

    mock_self.ticker_blacklist = {"XYZ": None}
    transactions = DirectIndexTaxLossStrategy._plan_transactions(
        mock_self, desired_portfolio=desired, current_portfolio=current
    )
    assert len(transactions) == 0


def test_init_strategy(config, monkeypatch):
    monkeypatch.setattr(
        DirectIndexTaxLossStrategy,
        "_load_yf_prices",
        lambda x, y, z: pd.DataFrame(
            {"A": [12.1, 12.3], "AA": [13.1, 13.4], "IVV": [100.0, 100.1]},
            index=pd.DatetimeIndex(["2006-09-29", "2006-10-02"], name="index"),
        ),
    )
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_load_current_portfolio", lambda x, y: Portfolio(cash=100.0))
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_load_ticker_blacklist", lambda x, y, z: {"A": None})
    monkeypatch.setattr(
        DirectIndexTaxLossStrategy, "_load_index_weights", lambda x, y, z: pd.Series([0.75, 0.25], index=["A", "AA"])
    )
    mock_gw = mock.Mock()
    mock_gw.get_market_prices = lambda tickers: {
        "A": MarketPrice(10.0, pd.Timestamp.now().to_pydatetime()),
        "AA": MarketPrice(12.0, pd.Timestamp.now().to_pydatetime()),
    }
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_init_gateway", lambda x, y: mock_gw)
    DirectIndexTaxLossStrategy(config)
