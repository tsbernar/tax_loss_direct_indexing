import abc
import logging
from time import time
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import OptimizeResult, minimize

logger = logging.getLogger(__name__)


class IndexOptimizer(abc.ABC):
    def __init__(self, index_returns, component_returns):
        """
        index_returns:
            Vector of index returns per date.  Optional date index.
            Ex:

            Date
            2021-01-04   -0.013772
            2021-01-05    0.006618
            ...

        component_returns:
            Matrix of returns of some stocks to be used to replicate the index.  Optional date index.
            Ex:

            Date            AAPL      ABBV       ABT
            2021-01-04 -0.024719 -0.016239 -0.003471
            2021-01-05  0.012364  0.010341  0.012373

        """
        self.index_returns = index_returns
        self.component_returns = component_returns

    @abc.abstractmethod
    def optimize(self):
        """
        Returns:

            weights: pd.Series
                weights of component returns.
                Ex:

                AMZN    0.0435
                MSFT    0.0526
                AAPL    0.0661

            full_result: Any
                Full native result from optimzer.

        """
        pass


class MinimizeOptimizer(IndexOptimizer):
    def __init__(
        self,
        index_returns,
        component_returns,
        true_index_weights,
        tax_coefficient,
        starting_portfolio,
        initial_weight_guess,
        max_deviation_from_true_weight=0.03,
        ticker_blacklist: Dict[str, Tuple[float, float]] = {},
        cash_constraint=0.95,
        tracking_error_func="least_squared",
    ):

        self.index_returns = index_returns
        self.component_returns = component_returns
        self.true_index_weights = true_index_weights
        self.tax_coefficient = tax_coefficient
        self.starting_portfolio = starting_portfolio
        self.initial_weight_guess = initial_weight_guess
        self.ticker_blacklist = ticker_blacklist
        self.max_deviation_from_true_weight = max_deviation_from_true_weight
        self.cash_constraint = cash_constraint
        self.tracking_error_func = tracking_error_func

    def _get_bounds(self, ticker, tw):
        if ticker in self.ticker_blacklist:
            return self.ticker_blacklist[ticker]  # TODO we should keep the min here even if in blacklist in some cases
        min_bound = max(0, tw - self.max_deviation_from_true_weight)
        max_bound = tw + self.max_deviation_from_true_weight
        return (min_bound, max_bound)

    @staticmethod
    def _tax_loss_pct_harvested(
        weights,
        ticker_indices,
        starting_portfolio,
        starting_portfolio_weights,
        starting_portfolio_prices,
        starting_portfolio_nav,
    ):
        # Tax loss is positive number, tax gain is negative number
        # returns tax losses harvested as a percentage of total portfolio value
        # TODO: later add option to optimize around fixed value harvested losses rather than portfolio size?
        # to keep more in line with expected taxable income rather than pf size which could be much bigger.

        total_tax_loss = 0
        hifo_time = 0

        share_changes = (weights - starting_portfolio_weights) / starting_portfolio_prices * starting_portfolio_nav
        share_changes = pd.DataFrame(
            {
                "share_change": share_changes,
                "ticker": ticker_indices,
                "market_price": starting_portfolio_prices,
            }
        )
        share_sells = share_changes[share_changes.share_change < 0]

        if len(share_sells):
            # THIS IS A BOTTLENECK, precompute hifo price and sahres for fixed share increments and look up in a matrix?
            t0 = time()
            hifo_basis_prices = share_sells.apply(
                lambda x: starting_portfolio.ticker_to_cost_basis[x.ticker].hifo_basis(-x.share_change).price,
                axis=1,
            )
            hifo_time = time() - t0

            tax_loss = (share_sells.market_price - hifo_basis_prices) * share_sells.share_change
            total_tax_loss = tax_loss.sum()

        return total_tax_loss / starting_portfolio_nav, hifo_time

    @staticmethod
    def _least_squared(index_returns, component_returns, weights):
        return np.square((index_returns - component_returns.dot(weights)) * 100).mean()

    @staticmethod
    def _var_tracking_diff(index_returns, component_returns, weights):
        tracking_diff = (index_returns - component_returns.dot(weights)) * 100
        return np.var(tracking_diff)

    @staticmethod
    def _make_cash_constraints(cash_constraint):
        # must use at least cash_constraint% of cash (this constraint functions should return >=0 when condition met)
        # can't use more than available cash (this constraint functions should return >=0 when condition met)
        cons = [
            {"type": "ineq", "fun": lambda x: x.sum() - cash_constraint},
            {"type": "ineq", "fun": lambda x: 1 - x.sum()},
        ]
        return cons

    @staticmethod
    def _score(x, *args):
        weights = x
        index_returns = args[0]
        component_returns = args[1]
        tax_coefficient = args[2]
        ticker_indices = args[3]
        tracking_error_func = args[4]
        starting_portfolio = args[5]
        times = args[6]
        starting_portfolio_weights = args[7]
        starting_portfolio_prices = args[8]
        starting_portfolio_nav = args[9]

        t0 = time()
        tracking_error = tracking_error_func(index_returns, component_returns, weights)
        times[0] += time() - t0
        t0 = time()
        tax_loss_harvested, hifo_time = MinimizeOptimizer._tax_loss_pct_harvested(
            weights,
            ticker_indices,
            starting_portfolio,
            starting_portfolio_weights,
            starting_portfolio_prices,
            starting_portfolio_nav,
        )
        times[1] += time() - t0
        times[2] += hifo_time
        times[4] += 1

        logger.debug(
            f"tracking_error: {tracking_error : .4f}, "
            f"tax_loss_harvested: {tax_loss_harvested : .4f}, "
            f"tax score: {tax_coefficient*tax_loss_harvested : .4f}, "
            f"total score: {tracking_error - tax_coefficient*tax_loss_harvested : .4f}, "
            f"tracking_error_time: {times[0] : .2f}s, tax_loss_time: {times[1] : .2f}s, "
            f"hifo_time: {times[2]: .2f}s, count: {times[4]}"
        )
        times[3] = time()
        return tracking_error - tax_coefficient * tax_loss_harvested

    def optimize(self) -> Tuple[pd.Series, OptimizeResult]:
        if self.tracking_error_func == "least_squared":
            func = self._least_squared
        elif self.tracking_error_func == "var_tracking_diff":
            func = self._var_tracking_diff
        else:
            raise ValueError(f"Unrecognized tracking_error_func: {self.tracking_error_func}")

        bounds = [self._get_bounds(ticker, tw) for ticker, tw in self.true_index_weights.iteritems()]
        cons = self._make_cash_constraints(self.cash_constraint)
        # TODO add constraint for sum abs diff from index
        # TODO add in penalty for churn
        # TODO some of this should be moved to init

        ticker_indices = list(self.initial_weight_guess.index)
        starting_portfolio_weights = [self.starting_portfolio.weight(ticker) for ticker in ticker_indices]
        starting_portfolio_prices = [
            self.starting_portfolio.ticker_to_market_price[ticker].price for ticker in ticker_indices
        ]
        starting_portfolio_nav = self.starting_portfolio.nav

        result = minimize(
            self._score,
            self.initial_weight_guess,
            args=(
                self.index_returns,
                self.component_returns,
                self.tax_coefficient,
                ticker_indices,
                func,
                self.starting_portfolio,
                [0, 0, 0, 0, 0, 0],
                starting_portfolio_weights,
                starting_portfolio_prices,
                starting_portfolio_nav,
            ),
            bounds=bounds,
            constraints=cons,
            options={"maxiter": 200}  # double the default max
            #  options={'eps':1e-8}
        )

        minimize_weights = pd.Series(result.x, index=self.initial_weight_guess.index).sort_values()
        return minimize_weights, result
