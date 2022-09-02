import abc
import datetime
import logging
from decimal import Decimal
from time import sleep
from typing import Dict, List, Optional, Sequence, Union

import munch
import pandas as pd
import requests

from tax_loss.portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from tax_loss.trade import FillStatus, Order, OrderStatus, Side, Trade

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

    @abc.abstractmethod
    def try_execute(self, desired_trades: Sequence[Trade]) -> List[Trade]:
        pass


class IBKRGateway(Gateway):
    def __init__(self, config: munch.Munch):
        self.credentials: munch.Munch = self._read_credentials(config.credentials_filename)
        self.base_url = config.base_url
        self.conid_filepath = config.conid_filepath
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
        logger.info(f"IBKR trades: {ibkr_trades}")

        trades = []
        for ibkr_trade in ibkr_trades:
            trades.append(self._decode_ibkr_trade(ibkr_trade))

        return trades

    def get_orders(self) -> List[Order]:
        endpoint = "/iserver/account/orders"
        response = self._make_request(method="GET", endpoint=endpoint)

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
        ticker_to_cost_basis = {
            str(p["contractDesc"]): self._decode_cost_basis_info(p) for p in positions if Decimal(p["position"]) != 0
        }
        # TODO : Use market data endpoint to get update with real "last_updated" ts?
        ticker_to_market_price = {str(p["contractDesc"]): self._decode_market_price(p) for p in positions}
        portfolio = Portfolio(
            cash=cash, ticker_to_cost_basis=ticker_to_cost_basis, ticker_to_market_price=ticker_to_market_price
        )
        return portfolio

    def try_execute(self, desired_trades: Sequence[Trade]) -> List[Trade]:
        orders = self._trades_to_orders(desired_trades)
        if not all([o.exchange_symbol for o in orders]):
            orders = self._add_conids(orders)
        sent_orders = self.submit_orders(orders)
        sent_order_ids = {o.id for o in sent_orders}
        #  Takes a while for orders to all show up on trades.. how long?
        logger.info("Waiting 1min before checking for trades")
        sleep(60)
        trades = self.get_trades()
        logger.debug(f"Got trades: {trades}")
        my_trades = [t for t in trades if t.order_id in sent_order_ids]
        logger.info(f"Got trades with matching IDs: {my_trades}")
        if len(my_trades) != len(sent_orders):
            logger.warn("Trade vs order count mismatch")  # maybe we get partial fills? need to handle better
        return my_trades

    def submit_orders(self, orders: Sequence[Order]) -> List[Order]:
        endpoint = f"/iserver/account/{self.account_id}/orders"
        #  We still have to submit orders one at a time on this endpoint unless doing child/parent orders
        order_responses = []
        for order in orders:
            json_data: Dict[str, List[Dict[str, Union[str, float, int]]]] = {"orders": [self._encode_ibkr_order(order)]}

            logger.info(f"Submitting order: {order} as json: {json_data}")
            response = self._make_request(method="POST", endpoint=endpoint, json_data=json_data)
            if not response.ok:
                logger.warn(f"Problem submitting order {order}")
                continue
            order_response = response.json()
            if "messageIds" in order_response[0]:
                order_response = self._reply_question(order_response[0])
            logger.info(f"Order submission response: {order_response}")
            order_responses += order_response

        id_to_submitted_map = {order.id: order for order in orders}

        updated_orders = []
        for order_response in order_responses:
            order = id_to_submitted_map[order_response["local_order_id"]]
            updated_orders.append(self._update_order(order, order_response))

        if len(updated_orders) != len(orders):
            logger.warn("Some orders not sent")

        return updated_orders

    def _reply_question(
        self, order_response: Dict[str, Union[str, List[str]]]
    ) -> List[Dict[str, Union[str, List[str]]]]:
        endpoint = "/iserver/reply/{replyid}"
        # Sometimes you need to answer questions about a submission, ex:
        # {'id': 'dd0227af-2de0-46fe-947d-1fd83f314e20',
        # 'message': ['You are submitting an order without market data.
        # We strongly recommend against this as it may result in erroneous
        # and unexpected trades.\nAre you sure you want to submit this order?'],
        # 'isSuppressed': False, 'messageIds': ['o354']}
        # Always answer yes .. :/
        responses = []
        logger.warn(f"Question when submitting order: {order_response}, answering yes")
        for replyid in order_response["messageIds"]:
            endpoint = endpoint.format(replyid)
            response = self._make_request(method="POST", endpoint=endpoint)
            responses += response.json()

        return responses

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
        endpoint = f"/portfolio/{self.account_id}/positions/invalidate"
        response = self._make_request(method="POST", endpoint=endpoint)
        if not response.ok:
            return False
        return response.json()["message"] == "success"

    def _add_conids(self, orders: List[Order]) -> List[Order]:
        df = pd.read_parquet(self.conid_filepath).set_index("ticker")
        logger.debug(f"Updating conids on orders: {orders}")
        verified_orders = []
        for order in orders:
            if order.symbol not in df.index:
                logger.warn(f"No conid found for {order}.  Removing")
                continue
            order.exchange_symbol = str(df.loc[order.symbol].conid)
            verified_orders.append(order)
        logger.debug(f"Updated conids on orders: {verified_orders}")
        return verified_orders

    @staticmethod
    def _trades_to_orders(trades: Sequence[Trade]) -> List[Order]:
        orders = []
        for trade in trades:
            order = Order(
                symbol=trade.symbol,
                qty=trade.qty,
                price=trade.price,
                side=trade.side,
                exchange_symbol=trade.exchange_symbol,
                status=OrderStatus.INACTIVE,
                fill_status=FillStatus.NOT_FILLED,
            )
            orders.append(order)
        return orders

    @staticmethod
    def _update_order(order: Order, order_resonse: Dict[str, str]) -> Order:
        order.exchange_order_id = order_resonse["order_id"]
        if order_resonse["order_status"] == "PreSubmitted":
            order.status = OrderStatus.PENDING_SUBMIT
            order.fill_status = FillStatus.NOT_FILLED
        elif order_resonse["order_status"] == "Filled":
            order.status = OrderStatus.CANCELLED
            order.fill_status = FillStatus.FILLED
        elif order_resonse["order_status"] == "Submitted":
            order.status = OrderStatus.ACTIVE
            order.fill_status = FillStatus.NOT_FILLED
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
        ibkr_order["conid"] = int(order.exchange_symbol)
        ibkr_order["secType"] = order.exchange_symbol + ":STK"  # only using stocks
        ibkr_order["cOID"] = str(order.id)
        # TODO config + allow for other options like limit orders at mid price eventually cross if no fill, etc.
        ibkr_order["orderType"] = "MKT"
        ibkr_order["tif"] = "IOC"
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
            exchange_trade_id=str(ibkr_trade["execution_id"]),
            order_id=str(ibkr_trade["order_ref"]),
        )
        return trade

    def _make_request(self, method: str, endpoint: str, json_data: Optional[Dict] = None) -> requests.Response:
        if method == "GET":
            response = requests.get(self.base_url + endpoint, json=json_data, verify=False)
        elif method == "POST":
            response = requests.post(self.base_url + endpoint, json=json_data, verify=False)
        else:
            raise NotImplementedError(f"Method {method}")

        if not response.ok:
            logger.warn(f"Problem with request to {endpoint}. {response} : {response.text}")

        return response

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
