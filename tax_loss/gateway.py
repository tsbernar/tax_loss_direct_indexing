import abc
import datetime
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Union

import munch
import pandas as pd
import requests

from .portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from .trade import FillStatus, Order, OrderStatus, Side, Trade

logger = logging.getLogger(__name__)


class Gateway(abc.ABC):
    @abc.abstractmethod
    def get_current_portfolio(self) -> Portfolio:
        pass

    @abc.abstractmethod
    def submit_orders(self, orders: Sequence[Order]) -> List[Order]:
        pass

    @abc.abstractmethod
    def get_trades(self) -> List[Trade]:
        pass


class IBKRGateway(Gateway):
    def __init__(self, credentials_filename: str):
        self.credentials: munch.Munch = self._read_credentials(credentials_filename)
        self.base_url = "https://localhost:5000/v1/api"  # TODO config
        if not self._check_auth_status():
            self._re_auth()
        self.account_id: str = self._get_account_id()

    @staticmethod
    def _read_credentials(credentials_filename: str) -> munch.Munch:
        pass

    def get_trades(self) -> List[Trade]:
        endpoint = "/iserver/account/trades"
        response = self._make_request(method="GET", endpoint=endpoint)
        ibkr_trades = response.json()
        logger.info("IBKR trades: {ibkr_trades}")

        trades = []
        for ibkr_trade in ibkr_trades:
            trades.append(self._decode_ibkr_trade(ibkr_trade))

        return trades

    def get_orders(self) -> List[Order]:
        endpoint = "iserver/account/orders"
        response = self._make_request(method="GET", endpoint=endpoint)
        if not response.ok:
            logger.warn(f"Problem getting orders {response} : {response.text}")

        ibkr_orders = response.json()["orders"]
        logger.info(f"Get orders response: {ibkr_orders}")
        orders = []
        for ibkr_order in ibkr_orders:
            orders.append(self._decode_ibkr_order(ibkr_order))
        return orders

    def get_current_portfolio(self) -> Portfolio:
        self._recalc_portfolio()
        cash = self._get_cash()
        positions = self._get_positions()
        # TODO this does not have full cost basis info!!!
        ticker_to_cost_basis = {str(p["contractDesc"]): self._decode_cost_basis_info(p) for p in positions}
        # TODO : Use market data endpoint to get update with real "last_updated" ts?
        ticker_to_market_price = {str(p["contractDesc"]): self._decode_market_price(p) for p in positions}
        portfolio = Portfolio(
            cash=cash, ticker_to_cost_basis=ticker_to_cost_basis, ticker_to_market_price=ticker_to_market_price
        )
        return portfolio

    def submit_orders(self, orders: Sequence[Order]) -> List[Order]:
        endpoint = f"/iserver/{self.account_id}/orders"
        json_data: Dict[str, List[Dict[str, Union[str, float, int]]]] = {"orders": []}
        for order in orders:
            json_data["orders"].append(self._encode_ibkr_order(order))

        logger.info("Submitting orders: {orders} as json: {json_data}")
        response = self._make_request(method="POST", endpoint=endpoint, json_data=json_data)
        if not response.ok:
            logger.warn(f"Problem submitting orders {response} : {response.text}")

        order_resonses = response.json()
        logger.info(f"Order submission response: {order_resonses}")
        id_to_submitted_map = {order.id: order for order in orders}

        updated_orders = []
        for order_response in order_resonses:
            order = id_to_submitted_map[order_response["local_order_id"]]
            updated_orders.append(self._update_order(order, order_response))
        return updated_orders

    def _get_cash(self) -> float:
        endpoint = f"/portfolio/{self.account_id}/summary"
        response = self._make_request(method="GET", endpoint=endpoint)
        portfolio_summary = response.json()
        return float(portfolio_summary["availablefunds"]["amount"])

    def _get_positions(self) -> List[Dict[str, Union[str, float, int]]]:
        endpoint = f"/portfolio/{self.account_id}/positions"
        response = self._make_request(method="GET", endpoint=endpoint)
        positions = response.json()
        return positions

    def _recalc_portfolio(self) -> bool:
        endpoint = f"portfolio/{self.account_id}/positions/invalidate"
        response = self._make_request(method="POST", endpoint=endpoint)
        if not response.ok:
            return False
        return response.json()["message"] == "success"

    @staticmethod
    def _update_order(order: Order, order_resonse: Dict[str, str]) -> Order:
        order.exchange_order_id = order_resonse["order_id"]
        if order_resonse["order_status"] == "PreSubmitted":
            order.status = OrderStatus.PENDING_SUBMIT
        else:
            logger.warn(f"Unknown order status: { order_resonse['order_status'] }")
        return order

    @staticmethod
    def _decode_market_price(position: Dict[str, Union[str, float, int]]) -> MarketPrice:
        return MarketPrice(price=float(position["mktPrice"]), last_updated=datetime.datetime.now())

    @staticmethod
    def _decode_cost_basis_info(position: Dict[str, Union[str, float, int]]) -> CostBasisInfo:
        return CostBasisInfo(
            ticker=str(position["contractDesc"]),
            tax_lots=[TaxLot(shares=Decimal(position["position"]), price=float(position["mktPrice"]))],
        )

    @staticmethod
    def _decode_ibkr_order(ibkr_order: Dict[str, Union[str, float, int]]) -> Order:
        """
        {'acct': 'DU5822420',
        'conidex': '265598',
        'conid': 265598,
        'orderId': 620601549,
        'cashCcy': 'USD',
        'sizeAndFills': '100',
        'orderDesc': 'Bought 100 Market DAY',
        'description1': 'AAPL',
        'ticker': 'AAPL',
        'secType': 'STK',
        'listingExchange': 'NASDAQ.NMS',
        'remainingQuantity': 0.0,
        'filledQuantity': 100.0,
        'companyName': 'APPLE INC',
        'status': 'Filled',
        'avgPrice': '160.03',
        'origOrderType': 'MARKET',
        'supportsTaxOpt': '1',
        'lastExecutionTime': '220830220149',
        'orderType': 'Market',
        'bgColor': '#FFFFFF',
        'fgColor': '#000000',
        'order_ref': '234',
        'timeInForce': 'CLOSE',
        'lastExecutionTime_r': 1661896909000,
        'side': 'BUY'}
        """
        # TODO add a comment like this with an example for all decode funcs
        if ibkr_order["status"] == "Filled":
            status = OrderStatus.CANCELLED
            fill_status = FillStatus.FILLED
        elif ibkr_order["status"] == "PreSubmitted":
            status = OrderStatus.PENDING_SUBMIT
            fill_status = FillStatus.NOT_FILLED
        else:
            logger.warn(f"Unknown orders status {ibkr_order['status']}")

        if ibkr_order["side"] == "BUY":
            side = Side.BUY
        elif ibkr_order["side"] == "SELL":
            side = Side.SELL
        else:
            side = Side.UNKNOWN

        return Order(
            symbol=str(ibkr_order["ticker"]),
            qty=Decimal(str(float(ibkr_order["remainingQuantity"]) + float(ibkr_order["filledQuantity"]))),
            price=Decimal(str(ibkr_order["limit_price"])) if "limit_price" in ibkr_order else Decimal(0),
            side=side,
            exchange_symbol=str(ibkr_order["conid"]),
            status=status,
            fill_status=fill_status,
            exchange_ts=pd.Timestamp(ibkr_order["lastExecutionTime_r"], unit="ms", tz="UTC").tz_convert(
                "America/Chicago"
            ),
            exchange_order_id=str(ibkr_order["orderId"]),
            id=str(ibkr_order["order_ref"]) if "order_ref" in ibkr_order else str(ibkr_order["orderId"]),
        )

    @staticmethod
    def _encode_ibkr_order(order: Order) -> Dict[str, Union[str, float, int]]:
        assert order.exchange_symbol is not None
        ibkr_order: Dict[str, Union[str, float, int]] = {}
        ibkr_order["conid"] = order.exchange_symbol
        ibkr_order["secType"] = order.exchange_symbol + ":STK"  # only using stocks
        ibkr_order["cOID"] = int(order.id)
        # TODO config + allow for other options like limit orders at mid price eventually cross if no fill, etc.
        ibkr_order["orderType"] = "MOC"
        ibkr_order["tif"] = "DAY"
        ibkr_order["side"] = "BUY" if order.side == Side.BUY else "SELL"
        ibkr_order["quantity"] = float(order.qty)
        return ibkr_order

    @staticmethod
    def _decode_ibkr_trade(ibkr_trade: Dict[str, Union[str, float]]) -> Trade:
        if ibkr_trade["side"] == "B":
            side = Side.BUY
        elif ibkr_trade["side"] == "S":
            side = Side.SELL
        else:
            side = Side.UNKNOWN

        trade = Trade(
            symbol=str(ibkr_trade["symbol"]),  # TODO map this?
            qty=Decimal(str(ibkr_trade["size"])),
            price=Decimal(str(ibkr_trade["price"])),
            side=side,
            exchange_symbol=str(ibkr_trade["conid"]),
            exchange_ts=pd.Timestamp(ibkr_trade["trade_time_r"], unit="ms", tz="UTC").tz_convert("America/Chicago"),
            exchange_trade_id=str(ibkr_trade['"execution_id"']),
            order_id=str(ibkr_trade["order_ref"]),
        )
        return trade

    def _make_request(self, method: str, endpoint: str, json_data: Optional[Dict] = None) -> requests.Response:
        if method == "GET":
            return requests.get(self.base_url + endpoint, json=json_data, verify=False)
        elif method == "POST":
            return requests.post(self.base_url + endpoint, json=json_data, verify=False)
        else:
            raise NotImplementedError(f"Method {method}")

    def _check_auth_status(self) -> bool:
        endpoint = "/iserver/auth/status"
        response = self._make_request(method="POST", endpoint=endpoint)
        if not response.ok:
            return False
        return response.json()["authenticated"]

    def _re_auth(self) -> bool:
        endpoint = "/iserver/reauthenticate"
        response = self._make_request(method="POST", endpoint=endpoint)
        if not response.ok:
            return False
        return response.json()["message"] == "triggered"

    def _get_account_id(self) -> str:
        endpoint = "/iserver/accounts"
        response = self._make_request(method="GET", endpoint=endpoint)
        logger.info(f"Got IBKR account {response.json()}")
        return response.json()["accounts"][0]
