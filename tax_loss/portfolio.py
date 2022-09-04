import datetime
import json
import logging
from copy import deepcopy
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import tabulate

from tax_loss.trade import Side, Trade

SHARE_QUANTIZE = "0.1"  # allow trading in 10ths of shares

logger = logging.getLogger(__name__)


@dataclass
class TaxLot:
    shares: Decimal
    price: float
    date: datetime.date

    def __init__(
        self,
        shares: Union[Decimal, int, float],
        price: float,
        date: Optional[datetime.date] = None,
    ):
        # if no date provided, use today
        self.shares = Decimal(shares).quantize(Decimal(SHARE_QUANTIZE))
        self.price = price
        if not date:
            date = datetime.date.today()
        self.date = date


@dataclass
class CostBasisInfo:
    ticker: str
    tax_lots: List[TaxLot]

    def __init__(self, ticker: str, tax_lots: List[TaxLot]) -> None:
        self.ticker = ticker  # TODO: remove ticker from this class? simplifies json and it is redundant now
        self.tax_lots = tax_lots
        self.sort()

    def jsonable(self) -> Dict[str, Union[str, List[Dict[str, Union[str, float]]]]]:
        result: Dict[str, Any] = {"ticker": self.ticker}
        result["tax_lots"] = [{"shares": str(t.shares), "price": t.price, "date": str(t.date)} for t in self.tax_lots]
        return result

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "CostBasisInfo":
        ticker = json_dict["ticker"]
        tax_lots = [TaxLot(t["shares"], t["price"], pd.to_datetime(t["date"]).date()) for t in json_dict["tax_lots"]]
        return CostBasisInfo(ticker, tax_lots)

    @property
    def total_basis(self) -> TaxLot:
        total_price = 0.0
        total_shares = Decimal("0")
        for tax_lot in self.tax_lots:
            total_price += float(tax_lot.shares) * tax_lot.price
            total_shares += tax_lot.shares

        return TaxLot(
            shares=total_shares,
            price=(total_price / float(total_shares)) if total_shares > 0 else 0.0,
        )

    @property
    def total_shares(self) -> Decimal:
        return Decimal(sum(tl.shares for tl in self.tax_lots))

    def sort(self):
        self.tax_lots.sort(key=lambda x: x.price, reverse=True)

    def total_loss_basis(self, price: float) -> TaxLot:
        # Returns a TaxLot for all shares with a basis lower than price
        total_price = 0.0
        total_shares = Decimal("0.0")

        for tax_lot in self.tax_lots:
            if tax_lot.price > price:
                total_shares += tax_lot.shares
                total_price += float(tax_lot.shares) * tax_lot.price

        return TaxLot(
            shares=total_shares,
            price=(total_price / float(total_shares)) if total_shares > 0 else 0.0,
        )

    # TODO add FIFO, LIFO handling if needed
    def hifo_basis(self, shares: Union[int, float, Decimal]) -> TaxLot:
        # TODO: implementation of this bound by date (ex: hifo basis only for tax lots that count as long term gains)
        shares_remaining = Decimal(shares).quantize(Decimal(SHARE_QUANTIZE))
        total_price = 0.0
        total_shares = Decimal("0.0")

        # TODO skip if sorted?
        self.sort()

        for tax_lot in self.tax_lots:
            if shares_remaining <= 0:
                break
            shares_from_lot = min(tax_lot.shares, shares_remaining)
            shares_remaining -= shares_from_lot
            total_price += float(shares_from_lot) * tax_lot.price
            total_shares += shares_from_lot

        return TaxLot(
            shares=total_shares,
            price=(total_price / float(total_shares)) if total_shares > 0 else 0.0,
        )


@dataclass
class MarketPrice:
    price: float
    last_updated: datetime.datetime

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "MarketPrice":
        price: float = json_dict["price"]
        last_updated: datetime.datetime = pd.to_datetime(json_dict["last_updated"]).to_pydatetime()
        return MarketPrice(price, last_updated)

    def jsonable(self) -> Dict[str, Union[float, str]]:
        return {"price": self.price, "last_updated": str(self.last_updated)}


class Portfolio:
    def __init__(
        self,
        filename: Optional[str] = None,
        cash: float = 0.0,
        ticker_to_cost_basis: Optional[Dict[str, CostBasisInfo]] = None,
        ticker_to_market_price: Optional[Dict[str, MarketPrice]] = None,
    ) -> None:
        self.cash = cash
        self.ticker_to_cost_basis = ticker_to_cost_basis if ticker_to_cost_basis is not None else {}
        self.ticker_to_market_price = ticker_to_market_price if ticker_to_market_price is not None else {}
        if filename:
            self._from_json_file(filename)

    @classmethod
    def from_weights(
        cls, weights: pd.Series, nav: float, ticker_to_market_price: Dict[str, "MarketPrice"]
    ) -> "Portfolio":
        logger.info(f"Constructing portfolio from weights:\n{weights.sort_values()}")
        assert weights.sum() <= 1.0 + 1e-6  # allow for some floating point errors
        pf = cls()
        pf.cash = nav
        pf.ticker_to_market_price = deepcopy(ticker_to_market_price)
        for ticker, weight in weights.items():
            price = pf.ticker_to_market_price[ticker].price
            shares = weight * nav / price
            # Round down to avoid using more than nav value
            shares = Decimal(shares).quantize(Decimal(SHARE_QUANTIZE), rounding=ROUND_DOWN)
            if shares > 0:
                pf.buy(ticker, shares, price)

        # Now go back and fill in extra shares with any extra cash
        under_weight_cash = {t: (w - pf.weight(t)) * nav for t, w in weights.items() if pf.weight(t) < w}
        logger.debug(f"Under weight cash values:\n{json.dumps(under_weight_cash,indent=2)}")
        after_buy = {
            t: (pf.ticker_to_market_price[t].price * float(SHARE_QUANTIZE) - c) for t, c in under_weight_cash.items()
        }

        for ticker, after_buy in sorted(after_buy.items(), key=lambda x: x[1]):
            price = pf.ticker_to_market_price[ticker].price
            if price * float(SHARE_QUANTIZE) < pf.cash:
                pf.buy(ticker, Decimal(SHARE_QUANTIZE), price)

        return pf

    def update(self, trades: List[Trade]) -> None:
        for trade in trades:
            if trade.side == Side.BUY:
                self.buy(ticker=trade.symbol, shares=trade.qty, price=float(trade.price), fee=float(trade.fee))
            elif trade.side == Side.SELL:
                self.sell(ticker=trade.symbol, shares=trade.qty, price=float(trade.price), fee=float(trade.fee))

    def _from_json_file(self, filename: str) -> None:
        with open(filename) as f:
            json_dict = json.load(f)
            for ticker, cost_basis_json in json_dict["ticker_to_cost_basis"].items():
                cost_basis_info = CostBasisInfo.from_json(cost_basis_json)
                self.ticker_to_cost_basis[ticker] = cost_basis_info

            for ticker, market_price_json in json_dict["ticker_to_market_price"].items():
                market_price = MarketPrice.from_json(market_price_json)
                self.ticker_to_market_price[ticker] = market_price

            self.cash = json_dict["cash"]

    def jsonable(
        self,
    ) -> Dict[str, Union[float, Dict[str, float], Dict[str, Union[str, List[Dict[str, Union[str, float]]]]]]]:
        json_dict: Dict[str, Any] = {
            "ticker_to_cost_basis": {},
            "ticker_to_market_price": {},
            "cash": self.cash,
        }
        for ticker, cost_basis in self.ticker_to_cost_basis.items():
            json_dict["ticker_to_cost_basis"][ticker] = cost_basis.jsonable()
        for ticker, market_price in self.ticker_to_market_price.items():
            json_dict["ticker_to_market_price"][ticker] = market_price.jsonable()

        return json_dict

    def to_json_file(self, filename: str, indent=2) -> None:
        with open(filename, "w") as f:
            json.dump(self.jsonable(), f, indent=indent)

    @property
    def nav(self) -> float:
        nav = self.cash
        for ticker, cost_basis_info in self.ticker_to_cost_basis.items():
            total_basis = cost_basis_info.total_basis
            if total_basis.shares > 0:
                if ticker not in self.ticker_to_market_price:
                    raise ValueError(f"No market price for {ticker}")
                nav += float(total_basis.shares) * self.ticker_to_market_price[ticker].price
        return float(nav)

    def market_value(self, ticker: str) -> float:
        return float(self.ticker_to_cost_basis[ticker].total_shares) * self.ticker_to_market_price[ticker].price

    def weight(self, ticker: str) -> float:
        if ticker not in self.ticker_to_cost_basis:
            return 0
        return self.market_value(ticker) / self.nav

    def buy(self, ticker: str, shares: Decimal, price: float, fee: float = 0.0) -> None:
        if ticker not in self.ticker_to_cost_basis:
            self.ticker_to_cost_basis[ticker] = CostBasisInfo(ticker, [])
        self.ticker_to_cost_basis[ticker].tax_lots.append(TaxLot(shares, price))
        self.cash -= float(shares) * price
        self.cash -= fee

    def sell(self, ticker: str, shares: Decimal, price: float, fee: float = 0.0) -> float:
        # returns realized gain/loss
        assert ticker in self.ticker_to_cost_basis
        assert shares <= self.ticker_to_cost_basis[ticker].total_shares

        total_basis = 0.0
        shares_remaining = shares
        self.ticker_to_cost_basis[ticker].sort()

        while self.ticker_to_cost_basis[ticker].total_shares and (
            shares_remaining >= self.ticker_to_cost_basis[ticker].tax_lots[0].shares
        ):
            sold = self.ticker_to_cost_basis[ticker].tax_lots.pop(0)
            shares_remaining -= sold.shares
            total_basis += float(sold.shares) * sold.price

        if shares_remaining:
            sold = self.ticker_to_cost_basis[ticker].tax_lots[0]
            sold.shares = sold.shares - shares_remaining
            total_basis += float(shares_remaining) * sold.price
            shares_remaining = Decimal(0)

        self.cash += float(shares) * price
        self.cash -= fee
        realized_gain = float(shares) * price - total_basis
        return realized_gain

    def _generate_positions_table(self, max_rows: int, loss_sorted: bool) -> List[Dict[str, str]]:
        if max_rows is None:
            max_rows = len(self.ticker_to_cost_basis)

        table = []
        for ticker, ci in list(self.ticker_to_cost_basis.items()):
            row: Dict[str, str] = {}
            row["ticker"] = ticker
            row["total_shares"] = str(ci.total_shares)
            if ticker in self.ticker_to_market_price:
                price = self.ticker_to_market_price[ticker].price
                loss_basis = ci.total_loss_basis(price)
                total_basis = ci.total_basis
                row["total_shares_with_loss"] = str(loss_basis.shares)
                row["total_gain/loss"] = f"${float(total_basis.shares)*(price - total_basis.price) : ,.2f}"
                row["market_price"] = f"${price : ,.2f}"
                row["market_value"] = f"${price*float(ci.total_shares)  : ,.2f}"
                row["%"] = f"{price*float(ci.total_shares)/self.nav*100 : ,.2f}"

            table.append(row)
        if loss_sorted:
            table.sort(
                key=lambda x: (float(x["total_gain/loss"][1:].replace(",", "")), -float(x["%"].replace(",", "")))
            )
        else:
            table.sort(key=lambda x: float(x["%"].replace(",", "")), reverse=True)
        return table[:max_rows]

    def head(self, max_rows=10, loss_sorted=True) -> str:
        ret = f"Portfolio:\n nav:  ${self.nav : ,.2f}\n cash: ${self.cash : ,.2f}\n\n  "
        ret += tabulate.tabulate(self._generate_positions_table(max_rows, loss_sorted), headers="keys").replace(
            "\n", "\n  "
        )
        return ret

    def __repr__(self) -> str:
        return self.head(None)

    def __str__(self) -> str:
        return self.head(None)
