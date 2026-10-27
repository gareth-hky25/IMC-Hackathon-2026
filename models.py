# models are auto-generated

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


@dataclass(frozen=True)
class Product:
    symbol: str
    tickSize: float
    startingPrice: float
    contractSize: float | None = None
    minPrice: float | None = None
    maxPrice: float | None = None


@dataclass(frozen=True)
class Trade:
    timestamp: str
    sessionId: str
    product: str
    aggressor: str
    volume: float
    price: float
    id: str | None = None
    buyer: str | None = None
    seller: str | None = None


class Side(StrEnum):
    BUY = 'BUY'
    SELL = 'SELL'


@dataclass(frozen=True)
class OrderRequest:
    product: str
    side: Side
    price: float
    volume: float
    targetUser: str | None = None


class Status(StrEnum):
    ACTIVE = 'ACTIVE'
    PART_FILLED = 'PART_FILLED'


@dataclass(frozen=True)
class OrderResponse:
    id: str
    status: Status
    product: str
    side: Side
    price: float
    volume: float
    filled: float
    user: str
    timestamp: str
    message: str | None = None
    targetUser: str | None = None


@dataclass(frozen=True)
class Order:
    price: float
    volume: int
    own_volume: int


@dataclass(frozen=True)
class OrderBook:
    product: str
    tick_size: float
    buy_orders: list[Order]
    sell_orders: list[Order]
