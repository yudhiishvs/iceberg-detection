"""
Core simulation engine for limit order book simulation.

This package contains the fundamental components:
- Order types (limit, market, iceberg, cancel)
- Order book with price-time priority
- Matching engine for trade execution
- Event queue for discrete event simulation
- Market simulator orchestrator
"""

from .order import (
    Order,
    LimitOrder,
    MarketOrder,
    CancelOrder,
    IcebergOrder,
    OrderSide,
    OrderType,
    OrderStatus,
)

from .order_book import (
    OrderBook,
    PriceLevel,
)

from .matching_engine import (
    MatchingEngine,
    Trade,
    OrderResult,
)

from .event_queue import (
    EventQueue,
    Event,
    EventType,
    PeriodicEvent,
)

from .market_simulator import (
    MarketSimulator,
    SimulationConfig,
    SimulationStats,
    create_default_simulator,
)

__all__ = [
    # Order types
    'Order',
    'LimitOrder',
    'MarketOrder',
    'CancelOrder',
    'IcebergOrder',
    'OrderSide',
    'OrderType',
    'OrderStatus',
    
    # Order book
    'OrderBook',
    'PriceLevel',
    
    # Matching engine
    'MatchingEngine',
    'Trade',
    'OrderResult',
    
    # Event queue
    'EventQueue',
    'Event',
    'EventType',
    'PeriodicEvent',
    
    # Simulator
    'MarketSimulator',
    'SimulationConfig',
    'SimulationStats',
    'create_default_simulator',
]

__version__ = '0.1.0'