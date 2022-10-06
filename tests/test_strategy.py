import copy
import datetime
from decimal import Decimal

import mock
import munch
import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time

from tax_loss.email import Emailer
from tax_loss.portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from tax_loss.strategy import DirectIndexTaxLossStrategy
from tax_loss.trade import Side


@pytest.fixture
def config():
    config = munch.munchify(
        {
            "ticker_blacklist_extra": [],
            "price_data_file": "tests/resources/yf_tickers.parquet",
            "portfolio_file": "tests/resources/portfolio.json",
            "weight_cache_file": "tests/resources/weights.json",
            "ticker_blacklist_file": "tests/resources/ticker_blacklist.json",
            "wash_sale_days": 31,
            "max_stocks": 100,
            "index_weight_file": "tests/resources/IVV_weights.parquet",
            "ibkr_vs_cache_pf_cash_diff_tolerance": 0.1,
            "optimizer": {
                "lookback_days": 100,
                "tax_coefficient": 0.4,
                "max_deviation_from_true_weight": 0.05,
                "cash_constraint": 0.9,
                "tracking_error_func": "least_squared",
                "max_total_deviation": 0.9,
            },
            "gateway": {},
            "secrets_filepath": "filename",
        }
    )
    return config


@pytest.fixture
def strategy(config, monkeypatch):
    #  always patch the email secrets
    monkeypatch.setattr(
        Emailer, "_read_config", lambda x, y: munch.Munch({"email_user": "test", "email_app_pwd": "test"})
    )
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_send_summary_email", lambda w, x, y, z: None)
    #  don't write out files for tests
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_cache_portfolio", lambda x, portfolio, filename: None)
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_cache_weights", lambda x, y: None)
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_update_and_cache_blacklist", lambda x, y: None)
    #  and mock the gw
    mock_gw = mock.Mock()
    with freeze_time("2022-09-19 10:00"):
        market_prices = (
            DirectIndexTaxLossStrategy._load_yf_prices(
                "dummy_self", filename=config.price_data_file, lookback_days=config.optimizer.lookback_days
            )
            .iloc[-1]
            .to_dict()
        )
        mock_gw.get_market_prices = lambda tickers: {
            t: MarketPrice(p, datetime.datetime.now()) for t, p in market_prices.items() if t in tickers
        }
        mock_gw.get_current_portfolio = lambda: Portfolio(filename=config.portfolio_file)
        mock_gw.try_execute = lambda desired_trades: desired_trades
        monkeypatch.setattr(DirectIndexTaxLossStrategy, "_init_gateway", lambda x, y: mock_gw)
        strategy = DirectIndexTaxLossStrategy(config)
    return strategy


@pytest.fixture
def strategy_patched(monkeypatch, config):
    cur_pf = Portfolio(cash=100.0, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})
    gw_pf = Portfolio(cash=100.1, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})

    monkeypatch.setattr(
        Emailer, "_read_config", lambda x, y: munch.Munch({"email_user": "test", "email_app_pwd": "test"})
    )

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
    monkeypatch.setattr(
        Emailer, "_read_config", lambda x, y: munch.Munch({"email_user": "test", "email_app_pwd": "test"})
    )
    #  don't write out files for tests
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_cache_portfolio", lambda x, portfolio, filename: None)
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_cache_weights", lambda x, y: None)
    monkeypatch.setattr(DirectIndexTaxLossStrategy, "_update_and_cache_blacklist", lambda x, y: None)
    return DirectIndexTaxLossStrategy(config)


def test_blacklisting(config):
    json_data = '{"AAPL" : "2022-08-15", "GME" : null, "VTI": "2022-08-16", "ABC" : "2022-08-17"}'
    mock_self = mock.Mock()
    with freeze_time("2022-08-16 10:00"), mock.patch("builtins.open", mock.mock_open(read_data=json_data)):
        ticker_blacklist = DirectIndexTaxLossStrategy._load_ticker_blacklist(
            mock_self, "dummy_filename", config.ticker_blacklist_extra
        )
        assert ticker_blacklist == {"GME": None, "ABC": datetime.date(2022, 8, 17)}
        ticker_blacklist = DirectIndexTaxLossStrategy._load_ticker_blacklist(mock_self, "dummy_filename", ["XYZ"])
        assert ticker_blacklist == {"GME": None, "ABC": datetime.date(2022, 8, 17), "XYZ": None}


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


def test_init_strategy(strategy_patched):
    assert isinstance(strategy_patched, DirectIndexTaxLossStrategy)


def test_validate_current_portfolio(config, strategy_patched):
    assert strategy_patched.current_portfolio.ticker_to_cost_basis["A"].total_shares == Decimal(10)
    assert strategy_patched.current_portfolio.cash == 100.1

    cur_pf = Portfolio(cash=101.1, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})
    strategy_patched.current_portfolio = cur_pf
    strategy_patched._update_market_prices()
    strategy_patched._validate_current_portfolio(config, cash_tolerance_pct=0.02)
    assert strategy_patched.current_portfolio.cash == 100.1
    assert (
        strategy_patched.current_portfolio.ticker_to_cost_basis
        == strategy_patched.gateway.get_current_portfolio().ticker_to_cost_basis
    )

    bad_gw_pf = Portfolio(cash=200.1, ticker_to_cost_basis={"A": CostBasisInfo("A", [TaxLot(10, 12.0)])})
    strategy_patched.gateway.get_current_portfolio = lambda: bad_gw_pf
    with pytest.raises(ValueError):
        assert strategy_patched._validate_current_portfolio(config, cash_tolerance_pct=0.02)


@pytest.mark.e2e
def test_end_to_end(config, pytestconfig, strategy: DirectIndexTaxLossStrategy):
    # sanity checks to make sure we have correct resources
    assert strategy.index_weights.index[0] == "AAPL"
    assert strategy.index_weights["AAPL"] == 0.0711
    assert strategy.ticker_blacklist["AAPL"] == pd.to_datetime("2022-10-21").date()
    assert strategy.price_matrix["AAPL"].iloc[-1] == 154.47999572753906
    assert strategy.price_matrix["AAPL"].index.max() == pd.to_datetime("2022-09-19")
    assert strategy.current_portfolio.ticker_to_cost_basis["ADBE"].total_shares == Decimal("4.9")
    assert strategy.current_portfolio.ticker_to_cost_basis["VZ"].total_shares == Decimal("45.3")
    assert strategy.current_portfolio.ticker_to_cost_basis["GOOG"].total_shares == Decimal("17")
    assert np.isclose(strategy.current_portfolio.cash, 1380.515)

    import warnings

    warnings.filterwarnings(
        "ignore", message="Values in x were outside bounds during a minimize step, clipping to bounds"
    )

    strategy.run(rebalance=True)

    # check a few to make sure that the optimization gives the expected result
    assert strategy.current_portfolio.ticker_to_cost_basis["VZ"].total_shares == Decimal("47.4")
    assert strategy.current_portfolio.ticker_to_cost_basis["GOOG"].total_shares == Decimal("17")
    assert np.isclose(strategy.current_portfolio.cash, 455.831)

    # Running with no rebalance and no cash balance change should produce no change
    old_pf = copy.deepcopy(strategy.current_portfolio)
    strategy.run(rebalance=False)

    for ticker, cb in old_pf.ticker_to_cost_basis.items():
        assert strategy.current_portfolio.ticker_to_cost_basis[ticker].total_shares == cb.total_shares

    for ticker, mp in old_pf.ticker_to_market_price.items():
        assert np.isclose(strategy.current_portfolio.ticker_to_market_price[ticker].price, mp.price)

    assert np.isclose(old_pf.cash, strategy.current_portfolio.cash)

    # Even if some market prices change
    strategy.current_portfolio.ticker_to_market_price["TSLA"].price += 20.1
    strategy.current_portfolio.ticker_to_market_price["MSFT"].price -= 12.0
    strategy.current_portfolio.ticker_to_market_price["AAPL"].price += 1.2
    strategy.run(rebalance=False)

    for ticker, cb in old_pf.ticker_to_cost_basis.items():
        assert strategy.current_portfolio.ticker_to_cost_basis[ticker].total_shares == cb.total_shares

    assert np.isclose(old_pf.cash, strategy.current_portfolio.cash)

    # If we add some cash we'll buy some more shares, check a few
    strategy.current_portfolio.cash += 1000
    strategy.run(rebalance=False)
    print(strategy.current_portfolio)

    assert strategy.current_portfolio.ticker_to_cost_basis["TSLA"].total_shares == old_pf.ticker_to_cost_basis[
        "TSLA"
    ].total_shares + Decimal("0.1")
    assert strategy.current_portfolio.ticker_to_cost_basis["NKE"].total_shares == old_pf.ticker_to_cost_basis[
        "NKE"
    ].total_shares + Decimal("0.2")
    assert strategy.current_portfolio.ticker_to_cost_basis["AMAT"].total_shares == old_pf.ticker_to_cost_basis[
        "AMAT"
    ].total_shares + Decimal("0.2")
