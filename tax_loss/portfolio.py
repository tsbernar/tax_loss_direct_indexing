from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd
import tabulate


class Portfolio:
    def __init__(self):
        self.cash = 0
        self.ticker_to_cost_basis: Dict[str, CostBasisInfo] = {}
        self.ticker_to_market_price: Dict[str, float] = {}

    @property
    def nav(self):
        nav = self.cash
        for ticker, cost_basis_info in self.ticker_to_cost_basis.items():
            total_basis = cost_basis_info.total_basis
            if total_basis.shares > 0:
                if ticker not in self.ticker_to_market_price:
                    raise ValueError(f"No market price for {ticker}")
                nav += total_basis.shares * self.ticker_to_market_price[ticker]
        return nav

    def market_value(self, ticker):
        return (
            self.ticker_to_cost_basis[ticker].total_shares
            * self.ticker_to_market_price[ticker]
        )

    def weight(self, ticker):
        if ticker not in self.ticker_to_cost_basis:
            return 0
        return self.market_value(ticker) / self.nav

    def buy(self, ticker, shares, price):
        if ticker not in self.ticker_to_cost_basis:
            self.ticker_to_cost_basis[ticker] = CostBasisInfo(ticker, [])
        self.ticker_to_cost_basis[ticker].tax_lots.append(TaxLot(shares, price))
        self.cash -= shares * price

    def sell(self, ticker, shares, price):
        # returns realized gain/loss
        assert ticker in self.ticker_to_cost_basis
        assert shares <= np.round(self.ticker_to_cost_basis[ticker].total_shares, 1)

        total_basis = 0
        shares_remaining = shares
        self.ticker_to_cost_basis[ticker].sort()

        while self.ticker_to_cost_basis[ticker].total_shares and (
            shares_remaining >= self.ticker_to_cost_basis[ticker].tax_lots[0].shares
        ):
            sold = self.ticker_to_cost_basis[ticker].tax_lots.pop(0)
            shares_remaining -= sold.shares
            total_basis += sold.shares * sold.price

        if not np.isclose(shares_remaining, 0):
            sold = self.ticker_to_cost_basis[ticker].tax_lots[0]
            sold.shares = np.round(sold.shares - shares_remaining, 1)
            total_basis += shares_remaining * sold.price
            shares_remaining = 0

        self.cash += shares * price
        realized_gain = shares * price - total_basis
        return realized_gain

    def _generate_positions_table(self, max_rows=None):
        if max_rows is None:
            max_rows = len(self.ticker_to_cost_basis)

        table = []
        for ticker, ci in list(self.ticker_to_cost_basis.items()):
            row = {}
            row["ticker"] = ticker
            row["total_shares"] = ci.total_shares
            if ticker in self.ticker_to_market_price:
                price = self.ticker_to_market_price[ticker]
                loss_basis = ci.total_loss_basis(price)
                row["total_shares_with_loss"] = loss_basis.shares
                row[
                    "total_loss"
                ] = f"${loss_basis.shares*(price - loss_basis.price) : ,.2f}"
                row["market_price"] = f"${price : ,.2f}"
                row["market_value"] = f"${price*ci.total_shares : ,.2f}"
                row["%"] = f"{price*ci.total_shares/self.nav*100 : ,.2f}"

            table.append(row)
        table.sort(key=lambda x: x["%"], reverse=True)
        return table[:max_rows]

    def head(self, max_rows=10):
        ret = f"Portfolio:\n nav:  ${self.nav : ,.2f}\n cash: ${self.cash : ,.2f}\n\n  "
        ret += tabulate.tabulate(
            self._generate_positions_table(max_rows), headers="keys"
        ).replace("\n", "\n  ")
        return ret

    def __repr__(self):
        return self.head(None)

    def __str__(self):
        return self.head(None)


@dataclass
class TaxLot:
    shares: int
    price: float
    date: pd.Timestamp

    def __init__(self, shares, price):
        # if no date provided, use today
        self.shares = shares
        self.price = price
        self.date = pd.to_datetime(pd.Timestamp.now().date())


@dataclass
class CostBasisInfo:
    ticker: str
    tax_lots: List[TaxLot]

    def __init__(self, ticker, tax_lots):
        self.ticker = ticker
        self.tax_lots = tax_lots
        self.sort()

    @property
    def total_basis(self):
        total_price = 0
        total_shares = 0
        for tax_lot in self.tax_lots:
            total_price += tax_lot.shares * tax_lot.price
            total_shares += tax_lot.shares

        return TaxLot(
            shares=total_shares,
            price=(total_price / total_shares) if total_shares > 0 else 0,
        )

    @property
    def total_shares(self):
        return sum(tl.shares for tl in self.tax_lots)

    def sort(self):
        self.tax_lots.sort(key=lambda x: x.price, reverse=True)

    def total_loss_basis(self, price: float):
        # Returns a TaxLot for all shares with a basis lower than price
        total_price = 0
        total_shares = 0

        for tax_lot in self.tax_lots:
            if tax_lot.price > price:
                total_shares += tax_lot.shares
                total_price += tax_lot.shares * tax_lot.price

        return TaxLot(
            shares=total_shares,
            price=(total_price / total_shares) if total_shares > 0 else 0,
        )

    # TODO add FIFO, LIFO handling if needed
    def hifo_basis(self, shares: int):
        # TODO: implementation of this bound by date (ex: hifo basis only for tax lots that count as long term gains)
        shares_remaining = shares
        total_price = 0
        total_shares = 0

        # TODO skip if sorted
        self.sort()

        for tax_lot in self.tax_lots:
            if shares_remaining <= 0:
                break
            shares_from_lot = min(tax_lot.shares, shares_remaining)
            shares_remaining -= shares_from_lot
            total_price += shares_from_lot * tax_lot.price
            total_shares += shares_from_lot

        return TaxLot(
            shares=total_shares,
            price=(total_price / total_shares) if total_shares > 0 else 0,
        )
