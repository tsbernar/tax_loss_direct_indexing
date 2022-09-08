import datetime
from decimal import Decimal

import mock
import munch
import pytest
from freezegun import freeze_time

from tax_loss.portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from tax_loss.strategy import DirectIndexTaxLossStrategy
from tax_loss.trade import Side


@pytest.fixture
def config():
    config = munch.Munch({"ticker_blacklist_extra": []})
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
