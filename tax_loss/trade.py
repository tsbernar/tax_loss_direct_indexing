import enum
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


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
    exchange_symbol: Optional[str] = None
    create_ts: int = field(init=False)
    exchange_ts: Optional[int] = None

    def __post_init__(self):
        self.create_ts = time.time_ns()


class OrderStatus(enum.Enum):
    INACTIVE = enum.auto()
    INSERT_PENDING = enum.auto()
    INSERT_FAILED = enum.auto()
    OPEN = enum.auto()
    CANCEL_PENDING = enum.auto()
    CANCELLED = enum.auto()


class FillStatus(enum.Enum):
    NOT_FILLED = enum.auto()
    PARTIAL_FILLED = enum.auto()
    FILLED = enum.auto()


@dataclass
class Order:
    qty: Decimal
    price: Decimal
    symbol: str
    exchange_symbol: Optional[str]
    status: OrderStatus
    fill_status: FillStatus
    create_ts: int = field(init=False)
    exchange_ts: Optional[int] = None

    def __post_init__(self):
        self.create_ts = time.time_ns()
