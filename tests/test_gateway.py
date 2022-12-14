import json
import time
from decimal import Decimal

import munch
import pandas as pd
import pytest
from freezegun import freeze_time

from tax_loss.gateway import IBKRGateway
from tax_loss.portfolio import MarketPrice
from tax_loss.trade import FillStatus, Order, OrderStatus, Side, Trade


@pytest.fixture
def config():
    config = munch.Munch()
    config.base_url = "http://test_base_url"
    config.conid_filepath = "conid_file"
    config.credentials_filename = "cred_file"
    return config


@pytest.fixture
def gateway(config, requests_mock, monkeypatch):
    requests_mock.post(config.base_url + "/iserver/auth/status", json={"authenticated": True})
    requests_mock.get(config.base_url + "/iserver/accounts", json={"accounts": ["1234"]})
    requests_mock.get(
        config.base_url + "/trsrv/secdef/schedule?assetClass=STK&symbol=AAPL&exchangeFilter=NASDAQ",
        json=[
            {
                "id": "p83132",
                "tradeVenueId": "v13200",
                "schedules": [
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
                        "tradingtimes": [
                            {"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}
                        ],
                    },
                    {
                        "clearingCycleEndTime": "2000",
                        "tradingScheduleDate": "20000104",
                        "sessions": [],
                        "tradingtimes": [
                            {"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}
                        ],
                    },
                    {
                        "clearingCycleEndTime": "2000",
                        "tradingScheduleDate": "20000105",
                        "sessions": [],
                        "tradingtimes": [
                            {"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}
                        ],
                    },
                    {
                        "clearingCycleEndTime": "2000",
                        "tradingScheduleDate": "20000106",
                        "sessions": [],
                        "tradingtimes": [
                            {"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}
                        ],
                    },
                    {
                        "clearingCycleEndTime": "2000",
                        "tradingScheduleDate": "20000107",
                        "sessions": [],
                        "tradingtimes": [
                            {"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}
                        ],
                    },
                    {
                        "clearingCycleEndTime": "0000",
                        "tradingScheduleDate": "20220905",
                        "sessions": [],
                        "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
                    },
                ],
            }
        ],
    )
    monkeypatch.setattr(IBKRGateway, "_get_conids", lambda x: {"ABC": "123", "XYZ": "456"})
    gw = IBKRGateway(config)
    return gw


@pytest.fixture
def order():
    order = Order(
        symbol="ABC",
        qty=Decimal("10"),
        price=Decimal("101.234"),
        side=Side.BUY,
        exchange_symbol="12345",
        status=OrderStatus.INACTIVE,
        fill_status=FillStatus.NOT_FILLED,
    )
    return order


@pytest.fixture
def trade():
    order = Trade(
        symbol="ABC",
        qty=Decimal("10"),
        price=Decimal("101.234"),
        side=Side.BUY,
        exchange_symbol="12345",
    )
    return order


def test_gateway_init(gateway, config, requests_mock):
    assert gateway.account_id == "1234"

    requests_mock.post(config.base_url + "/iserver/auth/status", json={"authenticated": False})
    assert not gateway._check_auth_status()
    requests_mock.post(config.base_url + "/iserver/auth/status", text="failed", status_code=404)
    assert not gateway._check_auth_status()


def test_get_trades(gateway, requests_mock):
    ibkr_trades = '[{"execution_id": "00012735.6311860f.01.01", "symbol": "INTU", "supports_tax_opt": "1", "side":\
    "B", "order_description": "Bot 0.5 @ 420.08 on IBKR", "trade_time": "20220902-18:59:31", "trade_time_r": \
    1662145171000, "size": 0.5, "price": "420.08", "order_ref": "d8f3af16-8505-4491-b7b2-86d56c627f61", \
    "exchange": "IBKR", "net_amount": 210.04, "account": "DU5822420", "accountCode": "DU5822420", \
    "company_name": "INTUIT INC", "contract_description_1": "INTU", "sec_type": "STK", "listing_exchange": \
    "NASDAQ.NMS", "conid": 270662, "conidEx": "270662", "directed_exchange": "IBKR", "clearing_id": "IB", \
    "clearing_name": "IB", "liquidation_trade": "0", "commission": "1.0"}, {"execution_id": "0000e22a.64bb6708.01.01", \
    "symbol": "MRK", "supports_tax_opt": "1", "side": "S", "order_description": "Sold 9 @ 86.41 on IEX", "trade_time": \
    "20220902-18:41:26", "trade_time_r": 1662144086000, "size": 9.0, "price": "86.41", "order_ref": \
    "7e0997a8-8969-4ed4-8975-359635901776", "exchange": "IEX", "commission": "1.02", "net_amount": 777.69, "account": \
    "DU5822420", "accountCode": "DU5822420", "company_name": "MERCK & CO INC", "contract_description_1": "MRK",\
    "sec_type": "STK", "listing_exchange": "NYSE", "conid": 70101545, "conidEx": "70101545", "directed_exchange": \
    "IEX", "clearing_id": "IB", "clearing_name": "IB", "liquidation_trade": "0"}]'

    ibkr_trades = json.loads(ibkr_trades)
    requests_mock.post(gateway.base_url + "/iserver/auth/status", json={"authenticated": False})

    requests_mock.get(gateway.base_url + "/iserver/account/trades", json=ibkr_trades)
    trades = gateway.get_trades()
    assert len(trades) == len(ibkr_trades)
    assert trades[0].side == Side.BUY
    assert trades[0].symbol == ibkr_trades[0]["symbol"]
    assert trades[0].price == Decimal("420.08")
    assert trades[0].order_id == ibkr_trades[0]["order_ref"]


def test_submit_orders(gateway, requests_mock, order):
    requests_mock.post(
        gateway.base_url + f"/iserver/account/{gateway.account_id}/orders",
        json=[
            {
                "order_id": "1483841810",
                "local_order_id": "f5d5b15a-fdf1-4059-b7ed-3a72b973694d",
                "order_status": "Filled",
                "encrypt_message": "1",
            }
        ],
    )
    order.id = "f5d5b15a-fdf1-4059-b7ed-3a72b973694d"
    orders = gateway.submit_orders([order])
    assert orders[0].id == "f5d5b15a-fdf1-4059-b7ed-3a72b973694d"
    assert orders[0].fill_status == FillStatus.FILLED
    requests_mock.post(
        gateway.base_url + f"/iserver/account/{gateway.account_id}/orders",
        json=[
            {
                "id": "dd0227af-2de0-46fe-947d-1fd83f314e20",
                "message": [
                    "You are submitting an order without market data.\
         We strongly recommend against this as it may result in erroneous\
         and unexpected trades.\nAre you sure you want to submit this order?"
                ],
                "isSuppressed": False,
                "messageIds": ["o354"],
            }
        ],
    )
    mocked = requests_mock.post(
        gateway.base_url + "/iserver/reply/o354",
        json=[
            {
                "order_id": "1483841810",
                "local_order_id": "f5d5b15a-fdf1-4059-b7ed-3a72b973694d",
                "order_status": "Filled",
                "encrypt_message": "1",
            }
        ],
    )
    orders = gateway.submit_orders([order])
    assert mocked.called
    assert mocked.last_request.json()["confirmed"]


def test_get_current_portfolio(gateway, requests_mock):
    requests_mock.post(
        gateway.base_url + f"/portfolio/{gateway.account_id}/positions/invalidate", json={"message": "success"}
    )
    requests_mock.get(
        gateway.base_url + f"/portfolio/{gateway.account_id}/summary",
        json={
            "totalcashvalue": {
                "amount": 1234.5,
                "currency": "USD",
                "isNull": False,
                "timestamp": 1662251021000,
                "value": None,
                "severity": 0,
            }
        },
    )
    requests_mock.get(
        gateway.base_url + f"/portfolio/{gateway.account_id}/positions",
        json=[
            {
                "acctId": "DU5822420",
                "conid": 265598,
                "contractDesc": "AAPL",
                "position": 19.1,
                "mktPrice": 155.66999815,
                "mktValue": 2973.3,
                "currency": "USD",
                "avgCost": 156.052356,
                "avgPrice": 156.052356,
                "realizedPnl": -271.36,
                "unrealizedPnl": -7.3,
                "exchs": None,
                "expiry": None,
                "putOrCall": None,
                "multiplier": None,
                "strike": 0.0,
                "exerciseStyle": None,
                "conExchMap": [],
                "assetClass": "STK",
                "undConid": 0,
            },
            {
                "acctId": "DU5822420",
                "conid": 118089500,
                "contractDesc": "ABBV",
                "position": 0.2,
                "mktPrice": 136.5,
                "mktValue": 27.3,
                "currency": "USD",
                "avgCost": 137.6933,
                "avgPrice": 137.6933,
                "realizedPnl": -2.6,
                "unrealizedPnl": -0.24,
                "exchs": None,
                "expiry": None,
                "putOrCall": None,
                "multiplier": None,
                "strike": 0.0,
                "exerciseStyle": None,
                "conExchMap": [],
                "assetClass": "STK",
                "undConid": 0,
            },
        ],
    )

    portfolio = gateway.get_current_portfolio()
    assert portfolio.ticker_to_cost_basis["ABBV"].total_shares == Decimal("0.2")
    assert portfolio.ticker_to_market_price["ABBV"].price == 136.5
    assert portfolio.cash == 1234.5


def test_get_market_prices(gateway, requests_mock):
    json_data = [
        {
            "conidEx": "1715006",
            "conid": 123,
            "_updated": 1662589447782,
            "server_id": "q0",
            "6119": "q0",
            "31": "131.43",
            "6509": "RivB",
            "7635": "131.43",
        },
        {
            "conidEx": "139673266",
            "conid": 456,
            "_updated": 1662590376596,
            "server_id": "q1",
            "6119": "q1",
            "31": "13.90",
            "6509": "RivB",
            "7635": "13.85",
        },
        {
            "conidEx": "4027",
            "conid": 4027,
            "_updated": 1662589447782,
            "server_id": "q2",
            "6119": "q2",
            "31": "178.26",
            "6509": "RivB",
            "7635": "178.30",
        },
        {
            "conidEx": "265598",
            "conid": 265598,
            "server_id": "q3",
            "_updated": 1662590413196,
            "6119": "q3",
            "31": "156.08",
            "6509": "RivB",
            "7635": "155.97",
        },
    ]
    requests_mock.get(
        gateway.base_url + "/iserver/marketdata/snapshot?conids=123,456&fields=7635",
        json=json_data,
    )
    assert gateway.get_market_prices() == {
        "ABC": MarketPrice(131.43, pd.to_datetime(1662589447782, unit="ms").to_pydatetime()),
        "XYZ": MarketPrice(13.85, pd.to_datetime(1662590376596, unit="ms").to_pydatetime()),
    }

    requests_mock.get(
        gateway.base_url + "/iserver/marketdata/snapshot?conids=123&fields=7635",
        json=json_data[:1],
    )
    assert gateway.get_market_prices(["ABC"]) == {
        "ABC": MarketPrice(131.43, pd.to_datetime(1662589447782, unit="ms").to_pydatetime())
    }


def test_check_market_open(gateway):
    with freeze_time("2022-09-06 10:00"):
        assert gateway.check_if_market_open()
    with freeze_time("2022-09-06 20:00"):
        assert not gateway.check_if_market_open()


def test_try_execute(gateway, trade, monkeypatch, order):
    monkeypatch.setattr(gateway, "submit_orders", lambda x: [order])
    monkeypatch.setattr(gateway, "get_trades", lambda: [trade])

    def sleep(seconds):
        pass

    monkeypatch.setattr(time, "sleep", sleep)
    trade.order_id = order.id
    with freeze_time("2022-09-06 10:00"):
        trades = gateway.try_execute([trade], wait=None)
        assert trades == [trade]
    with freeze_time("2022-09-06 20:00"):
        trades = gateway.try_execute([trade], wait=None)
        assert trades == []
