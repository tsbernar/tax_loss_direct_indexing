import datetime
import json
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import munch
import numpy as np
import pandas as pd
from scipy.optimize import OptimizeResult

from tax_loss.db import get_trades
from tax_loss.email import Emailer
from tax_loss.gateway import Gateway, IBKRGateway
from tax_loss.optimizer import IndexOptimizer, MinimizeOptimizer
from tax_loss.portfolio import Portfolio
from tax_loss.trade import Side, Trade
from tax_loss.util import repair_portfolio

INDEX_TICKER = "IVV"
DRY_RUN = "dry_run"
PX_PRECISION = "0.01"
logger = logging.getLogger(__name__)


class DirectIndexTaxLossStrategy:
    def __init__(self, config: munch.Munch) -> None:
        self.weight_cache_file = config.weight_cache_file
        self.is_dry_run = DRY_RUN in config
        self.portfolio_file = config.portfolio_file
        self.wash_sale_days = config.wash_sale_days
        self.ticker_blacklist_file = config.ticker_blacklist_file

        self.emailer = Emailer(secrets_filepath=config.secrets_filepath)
        self.price_matrix = self._load_yf_prices(config.price_data_file, config.optimizer.lookback_days)
        self.current_portfolio = self._load_current_portfolio(config.portfolio_file)
        self.ticker_blacklist: Dict[str, Optional[datetime.date]] = self._load_ticker_blacklist(
            self.ticker_blacklist_file, config.ticker_blacklist_extra
        )
        self.index_weights = self._load_index_weights(config.index_weight_file, config.max_stocks)
        self.gateway = self._init_gateway(config.gateway)
        self._update_market_prices()
        self._validate_current_portfolio(config, float(config.ibkr_vs_cache_pf_cash_diff_tolerance))
        self.optimizer = self._init_optimizer(config.optimizer)

        if self.is_dry_run:
            self.desired_portfolio_file = config[DRY_RUN].desired_portfolio_file
            self.rotate_desired_current = config[DRY_RUN].rotate_desired_current

    def run(self, rebalance: bool = True) -> None:
        """On a rebalance run, we recalucate our portfolio optimization.
        On a no rebalance run, we just keep the same portfolio weighting but can still do some trades,
        we only do buys to avoid adding to washsale backlist, main usecase to invest a new cash deposit."""
        if not rebalance:
            weights = self._read_and_update_cached_weights()
            desired_portfolio = Portfolio.from_weights_and_starting_pf(
                weights=weights, starting_pf=self.current_portfolio, blacklist=list(self.ticker_blacklist.keys())
            )

        elif rebalance:
            weights, _ = self._optimize()
            desired_portfolio = Portfolio.from_weights(
                weights=weights,
                nav=self.current_portfolio.nav,
                ticker_to_market_price=self.current_portfolio.ticker_to_market_price,
                blacklist=list(self.ticker_blacklist.keys()),
            )

        logger.info(f"Desired portfolio:\n{desired_portfolio}")
        desired_trades = self._plan_transactions(
            desired_portfolio=desired_portfolio, current_portfolio=self.current_portfolio
        )
        if self.is_dry_run:
            executed_trades = self._dry_run(desired_portfolio, desired_trades)
        else:
            executed_trades = self._wet_run(desired_trades)

        self._update_and_cache_blacklist(executed_trades)
        self._cache_weights(weights)
        self._send_summary_email(self.current_portfolio, executed_trades, rebalance)

    def _send_summary_email(
        self, current_portfolio: Portfolio, executed_trades: List[Trade], is_rebalance: bool
    ) -> None:
        self.emailer.send_summary_msg(
            executed_trades=executed_trades,
            current_portfolio=current_portfolio,
            is_dry_run=self.is_dry_run,
            is_rebalance=is_rebalance,
        )

    def _optimize(self) -> Tuple[pd.Series, OptimizeResult]:
        logger.info("Running optimization")
        t0 = time.time()
        weights, result = self.optimizer.optimize()
        elapsed = time.time() - t0
        logger.info(f"Optimization took {elapsed : .2f}s")

        logger.info(f"Weights: \n{weights.sort_values()}")
        logger.info(f"Total deviation from index: {abs(weights - self.index_weights).sum()}")
        logger.info(f"Total weight: {weights.sum()}")
        logger.debug(f"Current pf nav: {self.current_portfolio.nav}")

        if not result.success:
            logger.critical(f"Failed optimization.. result: {result}")
            raise ValueError(f"Failed Optimization.. result: {result}")
        return weights, result

    def _wet_run(self, desired_trades: List[Trade]) -> List[Trade]:
        executed_trades = self.gateway.try_execute(desired_trades)
        self._rotate_current_portfolio()
        logger.info("Updating portfolio with trades")
        self.current_portfolio.update(executed_trades)
        logger.info(f"Current portfolio: {self.current_portfolio}")
        logger.info(f"Current portfolio nav: {self.current_portfolio.nav}")
        self._cache_portfolio(portfolio=self.current_portfolio, filename=self.portfolio_file)
        return executed_trades

    def _dry_run(self, desired_portfolio: Portfolio, desired_trades: List[Trade]) -> List[Trade]:
        logger.info(f"Saving desired portfolio to {self.desired_portfolio_file}")
        self._cache_portfolio(portfolio=desired_portfolio, filename=self.desired_portfolio_file)
        if self.rotate_desired_current:
            self._rotate_current_portfolio()
            # for dry run, assume we can execute all trades at current prices
            logger.info("Updating portfolio with trades")
            self.current_portfolio.update(desired_trades)
            logger.info(f"Current portfolio: {self.current_portfolio}")
            self._cache_portfolio(portfolio=self.current_portfolio, filename=self.portfolio_file)
        return desired_trades

    def _rotate_current_portfolio(self):
        filename = self.portfolio_file + pd.Timestamp.now().strftime("%Y%m%d_%H%M")
        if ".json" in filename:  # move extension to the end
            filename = filename.replace(".json", "") + ".json"
        logger.info(f"Rotating last portfolio to {filename}")
        self._cache_portfolio(portfolio=self.current_portfolio, filename=filename)

    def _load_current_portfolio(self, filename: str) -> Portfolio:
        logger.info(f"Loading current portfolio from {filename}")
        current_portfolio = Portfolio(filename=filename)
        logger.debug(f"Current portfolio: {current_portfolio}")
        return current_portfolio

    def _validate_current_portfolio(self, config: munch.Munch, cash_tolerance_pct: float) -> None:
        ibkr_portfolio = self.gateway.get_current_portfolio()
        cash_diff_tolerance = self.current_portfolio.nav * cash_tolerance_pct
        error_flag = False

        if ibkr_portfolio.positions != self.current_portfolio.positions:
            logger.critical(
                "Current portfolio positions do not match IBKR portfolio!\n"
                f"current:\n{self.current_portfolio}\nibkr:\n{ibkr_portfolio}"
            )
            logger.info("Trying to repair portfolio")
            trades = get_trades(config=config, start_ts=pd.Timestamp.now() - pd.Timedelta("7 days"))
            new_pf = repair_portfolio(
                stale_portfolio=self.current_portfolio, target_portfolio=ibkr_portfolio, trades=trades
            )
            if new_pf is None:
                error_flag = True
                logger.critical("Could not repair")
            else:
                self.current_portfolio = new_pf
                logger.info(f"Repaired successfully. Current portfolio: \n{self.current_portfolio}")

        if abs(ibkr_portfolio.cash - self.current_portfolio.cash) > cash_diff_tolerance:
            logger.critical(
                f"Current portfolio is not within tolerance of IBKR cash balance. Tolerance {cash_diff_tolerance : .2f}"
            )
            error_flag = True

        if error_flag:
            raise ValueError("IBKR vs Current portfolio")

        cash_adjustment = ibkr_portfolio.cash - self.current_portfolio.cash
        logger.info(f"Updating cash by ${cash_adjustment: .2f}")
        self.current_portfolio.cash += cash_adjustment
        #  TODO save these cash adjustments in a DB table, use to figure out deposits if can't get that easily?

    def _update_market_prices(self) -> None:
        logger.debug("Updating market prices")
        latest_prices = self.gateway.get_market_prices(tickers=self.index_weights.index)
        self.current_portfolio.ticker_to_market_price.update(latest_prices)
        logger.info(f"Current portfolio: {self.current_portfolio}")

    def _cache_portfolio(self, portfolio: Portfolio, filename: str) -> None:
        portfolio.to_json_file(filename=filename)

    def _cache_weights(self, weights: pd.Series) -> None:
        filename = self.weight_cache_file + pd.Timestamp.now().strftime("%Y%m%d_%H%M")
        if ".json" in filename:  # move extension to the end
            filename = filename.replace(".json", "") + ".json"
        filenames = [self.weight_cache_file, filename]
        logger.info(f"Caching weights with market prices to {filenames}")

        df = pd.DataFrame(weights, columns=["weight"])
        df["market_price"] = pd.Series({t: m.price for t, m in self.current_portfolio.ticker_to_market_price.items()})
        logger.info(df)
        for fn in filenames:
            with open(fn, "w") as f:
                f.write(df.to_json(indent=2))

    def _read_and_update_cached_weights(self) -> pd.Series:
        # Market prices may have moved, we want to stay equal weighted by market cap from last rebalance
        # Recalculate new weights based on price changes.  Should have no new trades expected unless
        # cash balance changes,for example from a new depsoit that we need to invest.
        # TODO: This (among other things) will break from share splits without manual intervention.
        df = pd.read_json(self.weight_cache_file)
        df["new_market_price"] = pd.Series(
            {t: m.price for t, m in self.current_portfolio.ticker_to_market_price.items()}
        )
        to_drop = df[np.isnan(df["new_market_price"])]
        if len(to_drop):
            logger.warning(f"Dropping tickers from cached weights with no market price or not in index:\n {to_drop}")
            for ticker in to_drop.index:
                if ticker not in self.index_weights.index:
                    logger.warning(f"{ticker} not in Index")
                else:
                    logger.warning(f"{ticker} in index, but missing price")
        df = df.drop(to_drop.index)
        # How much portfolio value has changed from market prices alone (ignoring any new cash transactions)
        ratio = (df.weight / df.market_price * df.new_market_price).sum() + 1 - df.weight.sum()
        # New weight, keeping same market cap weight as before
        df["new_weight"] = (df.weight * df.new_market_price / df.market_price) / ratio
        df.index.name = "Ticker"
        logger.info(f"Read and update cached weights from {self.weight_cache_file}:\n{df}")
        return df["new_weight"].rename()

    def _update_and_cache_blacklist(self, executed_trades: List[Trade]) -> None:
        logger.info(f"Updating blacklist with {len(executed_trades)} trades")
        end_date = (pd.Timestamp.now() + pd.Timedelta(f"{self.wash_sale_days} days")).date()
        for trade in executed_trades:
            if trade.side != Side.SELL:
                continue
            if trade.symbol not in self.ticker_blacklist:
                self.ticker_blacklist[trade.symbol] = end_date
                continue
            current_end_date = self.ticker_blacklist[trade.symbol]
            if (current_end_date is not None) and end_date > current_end_date:
                self.ticker_blacklist[trade.symbol] = end_date

        jsonable = {t: (str(v) if v is not None else v) for t, v in self.ticker_blacklist.items()}
        logger.info(f"Caching blacklist to {self.ticker_blacklist_file}")
        with open(self.ticker_blacklist_file, "w") as f:
            json.dump(jsonable, f, indent=2)

    def _load_ticker_blacklist(
        self, filename: str, ticker_blacklist_extra: List[str]
    ) -> Dict[str, Optional[datetime.date]]:
        logger.debug(f"Loading ticker blacklist from {filename}")
        with open(filename) as f:
            ticker_to_expiry: Dict[str, Optional[str]] = json.loads(f.read())

        ticker_blacklist: Dict[str, Optional[datetime.date]] = {}
        for ticker, expiry_str in ticker_to_expiry.items():
            if expiry_str is None:
                ticker_blacklist[ticker] = None
            elif pd.to_datetime(expiry_str).date() > datetime.date.today():
                ticker_blacklist[ticker] = pd.to_datetime(expiry_str).date()

        logger.info(f"Adding extra ticker blacklist from config: {ticker_blacklist_extra}")
        ticker_blacklist.update({t: None for t in ticker_blacklist_extra})
        logger.info(f"Ticker blacklist: {ticker_blacklist}")
        return ticker_blacklist

    def _load_yf_prices(self, filename: str, lookback_days: int) -> pd.DataFrame:
        start_date = datetime.date.today() - pd.Timedelta(f"{lookback_days} days")
        logger.info(f"Loading yf price data from {start_date}")
        price_matrix = pd.read_parquet(filename)
        price_matrix = price_matrix[start_date:].dropna(axis=1)
        logger.debug(f"Price matrix:\n{price_matrix}")
        return price_matrix

    def _load_index_weights(self, filename: str, max_stocks: int) -> pd.Series:
        # returns a series with ticker as index and weight as values
        logger.info(f"Loading index weights from {filename}, max_stocks: {max_stocks}")
        df = pd.read_parquet(filename)
        # we only care about the lastest weights.. TODO: keep a seperate file cached with just latest date?
        date = df.date.max()
        weights = df[df.date == date].groupby(["Ticker"])["Weight (%)"].sum().reset_index()
        weights = weights.sort_values("Weight (%)").tail(max_stocks)
        weights = weights.set_index("Ticker")["Weight (%)"].sort_index()
        weights = weights / 100  # convert from % value
        weights[INDEX_TICKER] = 0

        YF_TICKER_CORRECTION_MAP = {  # To match IBKR
            "BRK-B": "BRK B",  # TODO: This is a hack, same const defined in download script.. centralize this mapping
        }

        weights = weights.rename(YF_TICKER_CORRECTION_MAP)

        logger.info(f"Loaded {len(weights)} index weights from {date}")
        logger.debug(weights)
        return weights

    def _make_index_returns(self, ticker: str) -> pd.DataFrame:
        return self.price_matrix[ticker].pct_change().tail(-1)

    def _make_component_returns(self) -> pd.DataFrame:
        return self.price_matrix.pct_change().tail(-1)

    def _drop_missing_tickers(
        self, component_returns: pd.DataFrame, index_weights: pd.Series
    ) -> Tuple[pd.DataFrame, pd.Series]:
        missing_tickers = [m for m in index_weights.index if m not in component_returns.columns]
        logger.info(f"Dropping tickers witih missing price data: {missing_tickers}")
        tickers = index_weights.index.drop(missing_tickers)
        component_returns = component_returns[tickers]
        index_weights = index_weights[tickers]
        return component_returns, index_weights

    def _plan_transactions(self, desired_portfolio: Portfolio, current_portfolio: Portfolio) -> List[Trade]:
        logger.info("Planning transactins")
        tax_gain = 0.0
        desired_tickers = set(desired_portfolio.ticker_to_cost_basis.keys())
        current_tickers = set(current_portfolio.ticker_to_cost_basis.keys())
        trades = []

        for ticker in desired_tickers.union(current_tickers):
            current_qty = Decimal()
            desired_qty = Decimal()
            if ticker in current_portfolio.ticker_to_cost_basis:
                current_qty = current_portfolio.ticker_to_cost_basis[ticker].total_shares
            if ticker in desired_portfolio.ticker_to_cost_basis:
                desired_qty = desired_portfolio.ticker_to_cost_basis[ticker].total_shares

            trade_qty = desired_qty - current_qty

            if trade_qty == 0:
                logger.info(f"No trade for {ticker}")
                continue

            # TODO maybe assume some slippage when making desired pf?
            trade_px = Decimal(desired_portfolio.ticker_to_market_price[ticker].price)
            trade_px = trade_px.quantize(Decimal(PX_PRECISION))
            side = Side.BUY if trade_qty > 0 else Side.SELL
            trade = Trade(qty=abs(trade_qty), price=trade_px, side=side, symbol=ticker)
            if trade.symbol in self.ticker_blacklist and trade.side == Side.BUY:
                logger.warning(f"Skipping desired trade {trade} because of blacklist")
                continue
            trades.append(trade)

            if side == Side.SELL:
                total_basis = current_portfolio.ticker_to_cost_basis[ticker].hifo_basis(-trade_qty)
                assert total_basis.shares == -trade_qty
                tax_gain += float(-trade_qty) * (float(trade_px) - total_basis.price)

        logger.info(f"Planned tax gain of ${tax_gain : .2f}")
        logger.debug(f"len desired trades: {len(trades)}")
        logger.info(f"Desired trades: \n{chr(10).join(map(str,trades))}")

        return trades

    def _init_gateway(self, config) -> Gateway:
        logger.info("Initializing gateway")
        gateway = IBKRGateway(config=config)
        return gateway

    def _init_optimizer(self, config: munch.Munch) -> IndexOptimizer:
        logger.info("Initializing optimizer")
        index_returns = self._make_index_returns(INDEX_TICKER)
        component_returns = self._make_component_returns()
        #  TODO add 0 weight for positions currently in pf but that are dropped from index?
        # So we don't have to immediately sell when they are dropped
        true_index_weights = self.index_weights
        component_returns, true_index_weights = self._drop_missing_tickers(component_returns, true_index_weights)

        tax_coefficient = float(config.tax_coefficient)
        starting_portfolio = self.current_portfolio
        initial_weight_guess = pd.Series(
            {ticker: self.current_portfolio.weight(ticker) for ticker in true_index_weights.index}
        )
        initial_weight_guess.index.name = true_index_weights.index.name

        max_deviation_from_true_weight = float(config.max_deviation_from_true_weight)

        # Do not increase posiiton for tickers in the blacklist
        ticker_blacklist = {ticker: (0.0, starting_portfolio.weight(ticker)) for ticker in self.ticker_blacklist}
        # Cap IVV weight to 10%  TODO: handle flipping between SPY, etc.
        if INDEX_TICKER not in ticker_blacklist:
            ticker_blacklist[INDEX_TICKER] = (0.0, 0.1)
        # Cant have bounds where min == max, so set 0 max to something small that will round to 0 shares.
        for ticker, weight_range in ticker_blacklist.items():
            if weight_range[0] == weight_range[1]:
                ticker_blacklist[ticker] = (weight_range[0], weight_range[1] + 1e-6)

        cash_constraint = config.cash_constraint
        tracking_error_func = config.tracking_error_func
        max_total_deviation = config.max_total_deviation

        optimizer = MinimizeOptimizer(
            index_returns=index_returns,
            component_returns=component_returns,
            true_index_weights=true_index_weights,
            tax_coefficient=tax_coefficient,
            starting_portfolio=starting_portfolio,
            initial_weight_guess=initial_weight_guess,
            max_deviation_from_true_weight=max_deviation_from_true_weight,
            ticker_blacklist=ticker_blacklist,
            cash_constraint=cash_constraint,
            tracking_error_func=tracking_error_func,
            max_total_deviation=max_total_deviation,
        )
        return optimizer
