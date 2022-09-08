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
            "ibkr_vs_cache_pf_cash_diff_tolerance": 0.1,
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


@pytest.fixture
def strategy(config, monkeypatch):
    cur_pf = Portfolio(cash=100.0, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})
    gw_pf = Portfolio(cash=100.1, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})

    monkeypatch.setattr(
        DirectIndexTaxLossStrategy,
        "_load_yf_prices",
        lambda x, y, z: pd.DataFrame(
            {"A": [12.1, 12.3], "AA": [13.1, 13.4], "IVV": [100.0, 100.1]},
            index=pd.DatetimeIndex(["2006-09-29", "2006-10-02"], name="index"),
        ),
    )
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_load_current_portfolio", lambda x, y: cur_pf)
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_load_ticker_blacklist", lambda x, y, z: {"A": None})
    monkeypatch.setattr(
        DirectIndexTaxLossStrategy, "_load_index_weights", lambda x, y, z: pd.Series([0.75, 0.25], index=["A", "AA"])
    )
    mock_gw = mock.Mock()
    mock_gw.get_market_prices = lambda tickers: {
        "A": MarketPrice(10.0, pd.Timestamp.now().to_pydatetime()),
        "AA": MarketPrice(12.0, pd.Timestamp.now().to_pydatetime()),
    }
    mock_gw.get_current_portfolio = lambda: gw_pf
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_init_gateway", lambda x, y: mock_gw)
    return DirectIndexTaxLossStrategy(config)


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


def test_init_strategy(strategy):
    assert isinstance(strategy, DirectIndexTaxLossStrategy)


def test_validate_current_portfolio(strategy):
    assert strategy.current_portfolio.ticker_to_cost_basis["A"].total_shares == Decimal(10)
    assert strategy.current_portfolio.cash == 100.1

    cur_pf = Portfolio(cash=101.1, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})
    strategy.current_portfolio = cur_pf
    strategy._update_market_prices()
    strategy._validate_current_portfolio(cash_tolerance_pct=0.02)
    assert strategy.current_portfolio.cash == 100.1
    assert (
        strategy.current_portfolio.ticker_to_cost_basis == strategy.gateway.get_current_portfolio().ticker_to_cost_basis
    )

    bad_gw_pf = Portfolio(cash=200.1, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})
    strategy.gateway.get_current_portfolio = lambda: bad_gw_pf
    with pytest.raises(ValueError):
        assert strategy._validate_current_portfolio(cash_tolerance_pct=0.02)
