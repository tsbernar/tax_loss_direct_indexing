import abc
import datetime
import logging
from decimal import Decimal
from time import sleep
from typing import Dict, List, Optional, Sequence, Set, Union, cast

import munch
import pandas as pd
import requests

from tax_loss.portfolio import CostBasisInfo, MarketPrice, Portfolio, TaxLot
from tax_loss.trade import FillStatus, Order, OrderStatus, Side, Trade
from tax_loss.util import Schedule

logger = logging.getLogger(__name__)

MARK_PRICE_FIELD = "7635"


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

    @abc.abstractmethod
    def get_market_prices(self, tickers: Optional[Sequence[str]] = None) -> Dict[str, MarketPrice]:
        pass


class IBKRGateway(Gateway):
    def __init__(self, config: munch.Munch):
        self.base_url = config.base_url
        self.conid_filepath = config.conid_filepath
        if not self._check_auth_status():
            self._re_auth()
        self.account_id: str = self._get_account_id()
        self.symbol_to_conid = self._get_conids()

    def get_trades(self) -> List[Trade]:
        ibkr_trades = self.get_ibkr_trades()
        trades = []
        for ibkr_trade in ibkr_trades:
            trades.append(self._decode_ibkr_trade(ibkr_trade))

        return trades

    def get_ibkr_trades(self) -> List[Dict[str, Union[str, float]]]:
        endpoint = "/iserver/account/trades"
        response = self._make_request(method="GET", endpoint=endpoint)
        ibkr_trades = response.json()
        logger.debug(f"IBKR trades: {len(ibkr_trades)}")
        return ibkr_trades

    def get_order(self, order_id: str) -> Order:
        endpoint = f"/iserver/account/order/status/{order_id}"
        response = self._make_request(method="GET", endpoint=endpoint)
        ibkr_order = response.json()
        logger.debug(f"IBKR order:{ibkr_order}")
        return self._decode_ibkr_order_status(ibkr_order)

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
        ticker_to_cost_basis = {
            str(p["contractDesc"]): self._decode_cost_basis_info(p) for p in positions if Decimal(p["position"]) != 0
        }
        ticker_to_market_price = {str(p["contractDesc"]): self._decode_market_price(p) for p in positions}
        portfolio = Portfolio(
            cash=cash, ticker_to_cost_basis=ticker_to_cost_basis, ticker_to_market_price=ticker_to_market_price
        )
        return portfolio

    def try_execute(
        self, desired_trades: Sequence[Trade], wait: Optional[float] = 60.0, get_trades_retries: int = 30
    ) -> List[Trade]:
        if not self.check_if_market_open():
            logger.warning("Market appears closed, skipping execute")
            return []
        orders = self._trades_to_orders(desired_trades)
        if not all([o.exchange_symbol for o in orders]):
            orders = self._add_conids_to_orders(orders)
        sent_orders = self.submit_orders(orders)
        sent_order_ids = {o.id for o in sent_orders}
        # TODO check order status for canceled orders
        # Takes a while for orders to all show up on trades..
        count = 0
        while count < get_trades_retries:
            logger.info(f"Waiting {wait}s before checking for trades")
            if wait:
                sleep(wait)
            count += 1
            trades = self.get_trades()
            logger.debug(f"Got trades: {len(trades)}")
            my_trades = [t for t in trades if t.order_id in sent_order_ids]
            my_trade_oids = {t.order_id for t in my_trades}
            logger.info(f"Got trades with matching order IDs: {len(my_trade_oids)}")
            if len(my_trade_oids) != len(sent_orders):
                logger.warning(f"Trade oid vs order count mismatch {len(my_trade_oids)} {len(sent_orders)}")
                continue
            break
        return my_trades

    def get_market_prices(self, tickers: Optional[Sequence[str]] = None) -> Dict[str, MarketPrice]:
        if tickers is None:
            tickers = list(self.symbol_to_conid.keys())
        #  Process in chunks of 200 to avoid URL being too long
        #  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/414
        start = 0
        chunk_size = 200
        result: Dict[str, MarketPrice] = {}

        while start < len(tickers):
            result.update(self._get_market_prices(tickers=tickers[start : start + chunk_size]))
            start += chunk_size

        return result

    def submit_orders(self, orders: Sequence[Order]) -> List[Order]:
        endpoint = f"/iserver/account/{self.account_id}/orders"
        #  We still have to submit orders one at a time on this endpoint unless doing child/parent orders
        order_responses = []
        for order in orders:
            json_data: Dict[str, List[Dict[str, Union[str, float, int]]]] = {"orders": [self._encode_ibkr_order(order)]}

            logger.info(f"Submitting order: {order} as json: {json_data}")
            response = self._make_request(method="POST", endpoint=endpoint, json_data=json_data)
            if not response.ok:
                logger.warning(f"Problem submitting order {order}")
                continue
            order_response = response.json()
            if "messageIds" in order_response[0]:
                order_response = self._reply_question(order_response[0])
            logger.info(f"Order submission response: {order_response}")
            order_responses += order_response

        id_to_submitted_map = {order.id: order for order in orders}

        updated_orders = []
        for order_response in order_responses:
            if not isinstance(order_response, dict) or ("local_order_id" not in order_response):
                logger.warning(f"Skipping order_response {order_response}")
                continue
            order = id_to_submitted_map[order_response["local_order_id"]]
            updated_orders.append(self._update_order(order, order_response))

        if len(updated_orders) != len(orders):
            logger.warning("Some orders not sent")

        return updated_orders

    def check_if_market_open(self) -> bool:
        #  https://www.interactivebrokers.com/api/doc.html#tag/Contract/paths/~1trsrv~1secdef~1schedule/get
        #  Hacky solution.. use AAPL on NASDAQ as a proxy
        #  Try to figure out if today is open from schedule
        endpoint = "/trsrv/secdef/schedule?assetClass=STK&symbol=AAPL&exchangeFilter=NASDAQ"
        response = self._make_request(method="GET", endpoint=endpoint)
        logger.info(f"Got trading schedule {response.json()}")
        schedule = Schedule(response.json()[0]["schedules"])
        return schedule.is_open(
            pd.Timestamp(datetime.datetime.now()).tz_localize(
                "America/Chicago"
            )  # Use datetime.now() so this plays nicely with freezegun for unit tests
        )  # TODO timezones in config

    def _get_market_prices(self, tickers: Optional[Sequence[str]] = None) -> Dict[str, MarketPrice]:
        endpoint = "/iserver/marketdata/snapshot?conids={conids}&fields={MARK_PRICE_FIELD}"
        if tickers is None:
            tickers = list(self.symbol_to_conid.keys())
        logger.info(f"Requesting market prices for {tickers}")

        conids_remaining = set()
        for t in tickers:
            if t not in self.symbol_to_conid:
                logger.warning(f"No conid found for {t}, skipping.")
                continue
            conids_remaining.add(self.symbol_to_conid[t])

        endpoint = endpoint.format(conids=",".join(sorted(conids_remaining)), MARK_PRICE_FIELD=MARK_PRICE_FIELD)
        result: Dict[str, MarketPrice] = {}
        conid_to_symbol = {c: s for s, c in self.symbol_to_conid.items()}
        requests = 0
        while conids_remaining:
            #  "To receive all available fields the /snapshot endpoint will need to be called several times"
            #  https://www.interactivebrokers.com/api/doc.html#tag/Market-Data/paths/~1iserver~1marketdata~1snapshot/get
            response = self._make_request(method="GET", endpoint=endpoint)
            for snapshot in response.json():
                self._process_market_data_snapshot(snapshot, conid_to_symbol, result, conids_remaining)

            requests += 1
            if requests >= 200:
                logger.warning("No market price found for some symbols after 200 requests, giving up.")
                logger.warning(f"Missing conids: {conids_remaining}")
                logger.warning(f"Missing tickers: {[conid_to_symbol[c] for c in conids_remaining]}")
                break
            sleep(0.05)

        return result

    def _reply_question(self, order_response: Dict[str, Union[str, List[str]]]) -> Dict[str, Union[str, List[str]]]:
        endpoint = "/iserver/reply/{replyid}"
        # Sometimes you need to answer questions about a submission, ex:
        # {'id': 'dd0227af-2de0-46fe-947d-1fd83f314e20',
        # 'message': ['You are submitting an order without market data.
        # We strongly recommend against this as it may result in erroneous
        # and unexpected trades.\nAre you sure you want to submit this order?'],
        # 'isSuppressed': False, 'messageIds': ['o354']}
        # Always answer yes .. :/
        logger.warning(f"Question when submitting order: {order_response}, answering yes")
        replyid = order_response["id"]
        endpoint = endpoint.format(replyid=replyid)
        response = self._make_request(method="POST", endpoint=endpoint, json_data={"confirmed": True})
        tries = 1
        while not response.ok and tries <= 3:
            tries += 1
            sleep(1)
            logger.info("Retrying..")
            response = self._make_request(method="POST", endpoint=endpoint, json_data={"confirmed": True})

        if response.ok:
            return response.json()
        return {}

    def _get_cash(self) -> float:
        endpoint = f"/portfolio/{self.account_id}/summary"
        response = self._make_request(method="GET", endpoint=endpoint)
        portfolio_summary = response.json()
        return float(portfolio_summary["totalcashvalue"]["amount"])

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

    def _add_conids_to_orders(self, orders: List[Order]) -> List[Order]:
        logger.debug(f"Updating conids on {len(orders)} orders")
        verified_orders = []
        for order in orders:
            if order.symbol not in self.symbol_to_conid:
                logger.warning(f"No conid found for {order}.  Removing")
                continue
            order.exchange_symbol = self.symbol_to_conid[order.symbol]
            verified_orders.append(order)
        logger.debug(f"Updated conids on {len(verified_orders)} orders")
        return verified_orders

    @staticmethod
    def _process_market_data_snapshot(
        snapshot: Dict[str, Union[str, int]],
        conid_to_symbol: Dict[str, str],
        result: Dict[str, MarketPrice],
        conids_remaining: Set[str],
    ) -> None:
        if MARK_PRICE_FIELD in snapshot:
            conid = str(snapshot["conid"])
            if conid not in conids_remaining:
                return

            last_updated = pd.to_datetime(snapshot["_updated"], unit="ms").to_pydatetime()
            mark_price = float(snapshot[MARK_PRICE_FIELD])
            symbol = conid_to_symbol[conid]
            result[symbol] = MarketPrice(mark_price, last_updated)
            conids_remaining.remove(conid)

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
            logger.warning(f"Unknown order status: { order_resonse['order_status'] }")
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
    def _decode_ibkr_order_status(ibkr_order_status: Dict[str, Union[None, bool, str, int]]) -> Order:
        """
        {'sub_type': None,
        'request_id': '46445',
        'order_id': 658253830,
        'conidex': '265598',
        'conid': 265598,
        'symbol': 'AAPL',
        'side': 'B',
        'contract_description_1': 'AAPL',
        'listing_exchange': 'NASDAQ.NMS',
        'option_acct': 'c',
        'company_name': 'APPLE INC',
        'size': '1.0',
        'total_size': '1.0',
        'currency': 'USD',
        'account': 'DU5822420',
        'order_type': 'MARKET',
        'cum_fill': '0.0',
        'order_status': 'Cancelled',
        'order_status_description': 'Order Cancelled',
        'tif': 'IOC',
        'fg_color': '#FFFFFF',
        'bg_color': '#AA0000',
        'order_not_editable': True,
        'editable_fields': '\x1e',
        'cannot_cancel_order': True,
        'deactivate_order': False,
        'sec_type': 'STK',
        'available_chart_periods': '#R|1',
        'order_description': 'Cancelled 1 Market IOC',
        'order_description_with_contract': '1 AAPL Market IOC',
        'alert_active': 1,
        'child_order_type': '0',
        'order_clearing_account': 'DU5822420',
        'size_and_fills': '0/1',
        'exit_strategy_display_price': '149.48',
        'exit_strategy_chart_description': 'Cancelled 1 Market IOC',
        'exit_strategy_tool_availability': '1',
        'allowed_duplicate_opposite': True,
        'order_time': '220928232950',
        'order_cancellation_by_system_reason': 'Cancelled by System:\n'}
        """

        if ibkr_order_status["order_status"] == "Cancelled":
            status = OrderStatus.CANCELLED
        elif ibkr_order_status["order_status"] == "Filled":
            status = OrderStatus.CANCELLED
        elif ibkr_order_status["order_status"] == "PreSubmitted":
            status = OrderStatus.PENDING_SUBMIT
        else:
            logger.warning(f"Unknown orders status {ibkr_order_status['order_status']}")

        filled_size = Decimal(cast(str, ibkr_order_status["cum_fill"]))
        size = Decimal(cast(str, ibkr_order_status["size"]))

        if filled_size == size:
            fill_status = FillStatus.FILLED
        elif filled_size > 0:
            fill_status = FillStatus.PARTIAL_FILLED
        elif filled_size == 0:
            fill_status = FillStatus.NOT_FILLED

        if ibkr_order_status["side"] == "B":
            side = Side.BUY
        elif ibkr_order_status["side"] == "S":
            side = Side.SELL
        else:
            side = Side.UNKNOWN

        return Order(
            symbol=cast(str, ibkr_order_status["symbol"]),
            qty=Decimal(cast(str, ibkr_order_status["total_size"])),
            price=Decimal(cast(str, ibkr_order_status["exit_strategy_display_price"])),
            side=side,
            exchange_symbol=str(ibkr_order_status["conid"]),
            status=status,
            fill_status=fill_status,
            exchange_ts=pd.Timestamp(ibkr_order_status["order_time"], unit="ms", tz="UTC").tz_convert(
                "America/Chicago"
            ),
            exchange_order_id=str(ibkr_order_status["order_id"]),
            id=str(ibkr_order_status["order_id"]),  # Client ID not returned on this endpoint
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
            logger.warning(f"Unknown orders status {ibkr_order['status']}")

        if ibkr_order["side"] == "BUY":
            side = Side.BUY
        elif ibkr_order["side"] == "SELL":
            side = Side.SELL
        else:
            side = Side.UNKNOWN

        return Order(
            symbol=str(ibkr_order["ticker"]),
            qty=Decimal(str(cast(float, ibkr_order["remainingQuantity"]) + cast(float, ibkr_order["filledQuantity"]))),
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
            fee=Decimal(str(ibkr_trade["commission"])),
            exchange_symbol=str(ibkr_trade["conid"]),
            exchange_ts=pd.Timestamp(ibkr_trade["trade_time_r"], unit="ms", tz="UTC").tz_convert("America/Chicago"),
            exchange_trade_id=str(ibkr_trade["execution_id"]),
            order_id=str(ibkr_trade["order_ref"]) if "order_ref" in ibkr_trade else None,
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
            logger.warning(f"Problem with request to {endpoint}. {response} : {response.text}")

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

    def _get_conids(self) -> Dict[str, str]:
        df = pd.read_parquet(self.conid_filepath).set_index("ticker")
        return dict(df.conid.astype(str))
