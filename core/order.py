"""
Order data structures for the limit order book simulator.

This module defines the base Order class and its variants:
- LimitOrder: Buy/sell at a specific price or better
- MarketOrder: Buy/sell at best available price
- CancelOrder: Remove a previously placed order
- IcebergOrder: Large order with hidden quantity
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    CANCEL = "CANCEL"
    ICEBERG = "ICEBERG"


class OrderStatus(Enum):
    PENDING = "PENDING"                     # Submitted but not yet processed
    ACTIVE = "ACTIVE"                       # In the order book
    FILLED = "FILLED"                       # Completely filled
    PARTIALLY_FILLED = "PARTIALLY_FILLED"   # Some quantity filled
    CANCELLED = "CANCELLED"                 # Cancelled by trader or system
    REJECTED = "REJECTED"                   # Rejected by matching engine


@dataclass
class Order:
    """
    Base class for all order types.
    
    Attributes:
        order_id: Unique identifier for this order
        timestamp: Simulation time when order was created
        trader_id: ID of the agent/trader placing the order
        side: BUY or SELL
        order_type: Type of order (LIMIT, MARKET, etc.)
        quantity: Number of shares/contracts
        filled_quantity: How much has been filled so far
        status: Current order status
    """
    order_id: str
    timestamp: float
    trader_id: str
    side: OrderSide
    order_type: OrderType = field(init=False)
    quantity: int
    filled_quantity: int = 0
    status: OrderStatus = OrderStatus.PENDING

    def __post_init__(self):
        """Validate order parameters"""
        if self.quantity <= 0:
            raise ValueError(f"Order quantity must be positive, got {self.quantity}")

    @property
    def remaining_quantity(self) -> int:
        """Unfilled quantity remaining"""
        return self.quantity - self.filled_quantity

    @property
    def is_filled(self) -> bool:
        """Check if order is completely filled"""
        return self.filled_quantity >= self.quantity

    @property
    def is_active(self) -> bool:
        """Check if order is active in the book"""
        return self.status == OrderStatus.ACTIVE

    def fill(self, quantity: int) -> int:
        """
        Fill a portion of this order.
        
        Args:
            quantity: Amount to fill
        Returns:
            Actual quantity filled (may be less than requested)
        """
        if quantity <= 0:
            raise ValueError(f"Fill quantity must be positive, got {quantity}")

        fillable = min(quantity, self.remaining_quantity)
        self.filled_quantity += fillable

        if self.is_filled:
            self.status = OrderStatus.FILLED
        elif self.filled_quantity > 0:
            self.status = OrderStatus.PARTIALLY_FILLED

        return fillable
    
    def cancel(self):
        """Cancel this order"""
        if self.status in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
            raise ValueError(f"Cannot cancel order with status {self.status}")
        self.status = OrderStatus.CANCELLED


@dataclass
class LimitOrder(Order):
    """
    Limit order: buy/sell at specified price or better.
    
    Buy limit orders execute at limit_price or lower.
    Sell limit orders execute at limit_price or higher.
    """
    price: float = 0.0
    
    def __post_init__(self):
        self.order_type = OrderType.LIMIT
        super().__post_init__()
        if self.price <= 0:
            raise ValueError(f"Limit price must be positive, got {self.price}")
    
    def __lt__(self, other):
        """
        Comparison for priority queue ordering.
        
        Buy orders: Higher price has priority, then earlier timestamp
        Sell orders: Lower price has priority, then earlier timestamp
        """
        if not isinstance(other, LimitOrder):
            return NotImplemented
        
        if self.side != other.side:
            raise ValueError("Cannot compare orders on different sides")
        
        if self.side == OrderSide.BUY:
            # Buy side: higher price first, then time priority
            if self.price != other.price:
                return self.price > other.price
            return self.timestamp < other.timestamp
        else:
            # Sell side: lower price first, then time priority
            if self.price != other.price:
                return self.price < other.price
            return self.timestamp < other.timestamp
    
    def can_match(self, other_price: float) -> bool:
        """
        Check if this order can match at the given price.
        
        Args:
            other_price: Price to check against
        Returns:
            True if order can execute at this price
        """
        if self.side == OrderSide.BUY:
            return other_price <= self.price
        else:
            return other_price >= self.price


@dataclass
class MarketOrder(Order):
    """
    Market order: execute immediately at best available price.
    
    Market orders have no price limit and will match against
    the best available prices in the order book.
    """
    
    def __post_init__(self):
        self.order_type = OrderType.MARKET
        super().__post_init__()

@dataclass
class CancelOrder(Order):
    """
    Cancel order: remove a previously placed order.
    
    Attributes:
        cancel_order_id: ID of the order to cancel
    """
    cancel_order_id: str = ""
    
    def __post_init__(self):
        self.order_type = OrderType.CANCEL
        self.quantity = 0


@dataclass
class IcebergOrder(LimitOrder):
    """
    Iceberg order: large order with hidden quantity.
    
    Only the 'visible_quantity' (tip) is shown in the order book.
    When the tip is filled, it automatically replenishes from
    the hidden 'peak_quantity' until the entire order is filled.
    
    Attributes:
        peak_quantity: Total order size (hidden)
        visible_quantity: Size of the visible tip
        min_visible_quantity: Minimum tip size (for randomization)
        max_visible_quantity: Maximum tip size (for randomization)
        randomize_refill: Whether to randomize tip size on refill
    """
    peak_quantity: int = 0                                              # Total hidden quantity
    visible_quantity: int = 0                                           # Current visible tip
    min_visible_quantity: Optional[int] = None
    max_visible_quantity: Optional[int] = None
    randomize_refill: bool = False
    _refill_count: int = field(default=0, init=False, repr=False)
    
    def __post_init__(self):
        """Initialize iceberg order with validation"""
        if self.peak_quantity <= 0:
            raise ValueError(f"Peak quantity must be positive, got {self.peak_quantity}")
        
        if self.visible_quantity <= 0:
            raise ValueError(f"Visible quantity must be positive, got {self.visible_quantity}")
        
        if self.visible_quantity > self.peak_quantity:
            raise ValueError(
                f"Visible quantity ({self.visible_quantity}) cannot exceed "
                f"peak quantity ({self.peak_quantity})"
            )
        
        self.quantity = self.visible_quantity

        super().__post_init__()
        self.order_type = OrderType.ICEBERG
        
        if self.min_visible_quantity is None:
            self.min_visible_quantity = self.visible_quantity
        if self.max_visible_quantity is None:
            self.max_visible_quantity = self.visible_quantity
    
    @property
    def total_filled_quantity(self) -> int:
        """Total quantity filled across all refills"""
        return self.filled_quantity
    
    @property
    def hidden_remaining(self) -> int:
        """Quantity still hidden (not yet made visible)"""
        return self.peak_quantity - self.filled_quantity - self.remaining_quantity
    
    @property
    def needs_refill(self) -> bool:
        """Check if the visible portion is filled and needs refill"""
        return (
            self.remaining_quantity == 0 and 
            self.hidden_remaining > 0 and
            self.status != OrderStatus.CANCELLED
        )
    
    def refill(self, new_visible_quantity: Optional[int] = None) -> bool:
        """
        Replenish the visible tip from hidden quantity.
        
        Args:
            new_visible_quantity: Override the refill size (optional)
            
        Returns:
            True if refilled, False if no hidden quantity remains
        """
        if not self.needs_refill:
            return False
        
        # Determine refill size
        if new_visible_quantity is None:
            new_visible_quantity = self.visible_quantity
        
        # Can't refill more than what's hidden
        refill_amount = min(new_visible_quantity, self.hidden_remaining)
        
        if refill_amount <= 0:
            self.status = OrderStatus.FILLED
            return False
        
        # Update quantity (this is what's visible in the book)
        self.quantity = refill_amount
        self.status = OrderStatus.ACTIVE
        self._refill_count += 1
        
        return True
    
    @property
    def refill_count(self) -> int:
        """Number of times this iceberg has been refilled"""
        return self._refill_count
    
    def __repr__(self):
        return (
            f"IcebergOrder(id={self.order_id}, side={self.side.value}, "
            f"price={self.price}, visible={self.quantity}/{self.visible_quantity}, "
            f"peak={self.peak_quantity}, filled={self.filled_quantity}, "
            f"hidden_remaining={self.hidden_remaining}, refills={self.refill_count})"
        )


AnyOrder = Order | LimitOrder | MarketOrder | CancelOrder | IcebergOrder