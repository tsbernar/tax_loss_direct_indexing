import json
from decimal import Decimal

import munch
import pytest

from tax_loss.gateway import IBKRGateway
from tax_loss.trade import FillStatus, Order, OrderStatus, Side


@pytest.fixture
def config():
    config = munch.Munch()
    config.base_url = "http://test_base_url"
    config.conid_filepath = "conid_file"
    config.credentials_filename = "cred_file"
    return config


@pytest.fixture
def gateway(config, requests_mock):
    requests_mock.post(config.base_url + "/iserver/auth/status", json={"authenticated": True})
    requests_mock.get(config.base_url + "/iserver/accounts", json={"accounts": ["1234"]})

    gw = IBKRGateway(config)
    return gw
    assert gw.account_id == "1234"


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
    assert mocked.last_request.json()["confirmed"] == True


# def test_get_current_portfolio()

# def test_get_orders()
