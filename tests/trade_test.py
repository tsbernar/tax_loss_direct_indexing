from decimal import Decimal
from typing import Optional

import pandas as pd
import pytest

from tax_loss.trade import Side, Trade


@pytest.mark.parametrize(
    "symbol, qty, price, id, expected",
    [
        ("AAPL", Decimal("97"), Decimal("157.32"), "1234", "1234"),
        ("GOOGL", Decimal("23"), Decimal("207.32"), None, None),
    ],
)
def test_trade_init(symbol: str, qty: Decimal, price: Decimal, id: Optional[str], expected: Optional[str]):
    trade = Trade(symbol=symbol, qty=qty, price=price, id=id, side=Side.BUY)
    assert type(trade.create_ts) == pd.Timestamp
    assert type(trade.id) == str
    assert trade.price == price
    assert trade.qty == qty
    if expected:
        assert trade.id == expected
