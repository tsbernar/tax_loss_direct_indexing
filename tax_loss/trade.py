import enum
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import pandas as pd


class Side(enum.Enum):
    BUY = 1
    SELL = -1
    UNKNOWN = 0


@dataclass
class Trade:
    symbol: str
    qty: Decimal
    price: Decimal
    side: Side
    fee: Decimal = Decimal(0)
    exchange_symbol: Optional[str] = None
    create_ts: pd.Timestamp = field(init=False)
    exchange_ts: Optional[pd.Timestamp] = None
    exchange_trade_id: Optional[str] = None
    order_id: Optional[str] = None
    id: Optional[str] = None

    def __post_init__(self):
        self.create_ts = pd.Timestamp.now(tz="America/Chicago")  # TODO: should be in config
        if self.id is None:
            self.id = str(uuid.uuid4())


class OrderStatus(enum.Enum):
    INACTIVE = enum.auto()
    INSERT_PENDING = enum.auto()
    INSERT_FAILED = enum.auto()
    ACTIVE = enum.auto()
    PENDING_SUBMIT = enum.auto()
    CANCEL_PENDING = enum.auto()
    CANCELLED = enum.auto()


class FillStatus(enum.Enum):
    NOT_FILLED = enum.auto()
    PARTIAL_FILLED = enum.auto()
    FILLED = enum.auto()


@dataclass
class Order:
    symbol: str
    qty: Decimal
    price: Decimal
    side: Side
    exchange_symbol: Optional[str]
    status: OrderStatus
    fill_status: FillStatus
    create_ts: int = field(init=False)
    exchange_ts: Optional[int] = None
    exchange_order_id: Optional[str] = None
    id: Optional[str] = None

    def __post_init__(self):
        self.create_ts = time.time_ns()
        if self.id is None:
            self.id = str(uuid.uuid4())
