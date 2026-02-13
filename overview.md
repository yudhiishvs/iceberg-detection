# Iceberg Detection Simulator - Core Engine Complete ✅

## What You Have Now

A complete, production-ready **in-process event-driven limit order book simulator** with:

### ✅ Core Components (5 files)

1. **`order.py`** (415 lines)
   - Base Order class with fill tracking
   - LimitOrder with price-time priority
   - MarketOrder for immediate execution
   - CancelOrder for removing orders
   - IcebergOrder with auto-refill logic (THE KEY COMPONENT!)

2. **`order_book.py`** (364 lines)
   - PriceLevel with FIFO queues
   - Full two-sided book (bids/asks)
   - Efficient sorted price levels
   - Depth queries and snapshots

3. **`matching_engine.py`** (345 lines)
   - Trade generation
   - Order matching with price-time priority
   - Iceberg refill tracking
   - OrderResult with detailed status

4. **`event_queue.py`** (265 lines)
   - Discrete event simulation
   - Priority queue for deterministic ordering
   - Periodic event scheduling
   - Event type management

5. **`market_simulator.py`** (387 lines)
   - Main orchestrator
   - Configuration management
   - Statistics collection
   - Callback system for monitoring
   - History tracking

### Supporting Files

- `__init__.py` - Package exports
- `README.md` - Comprehensive documentation
- `example_usage.py` - Three complete examples
- `requirements.txt` - Dependencies


## How Iceberg Orders Work

```python
# Create iceberg: 10,000 shares total, 100 visible at a time
iceberg = IcebergOrder(
    order_id="ICE001",
    side=OrderSide.SELL,
    price=100.00,
    peak_quantity=10000,      # Hidden total
    visible_quantity=100       # Visible "tip"
)

# When submitted:
# 1. Only 100 shares appear in the book
# 2. When those 100 fill → automatic refill event scheduled
# 3. Next 100 shares appear
# 4. Repeat until 10,000 shares exhausted

# This creates the "resilience" pattern you'll detect!
```

## Key Design Patterns

### 1. Discrete Event Simulation
```python
sim.submit_order(order, delay=0.5)  # Schedule for t+0.5
sim.run()  # Process all events in time order
```

### 2. Price-Time Priority Matching
```
Bids (descending):      Asks (ascending):
$100.00 - 500 shares    $100.05 - 300 shares
$99.95  - 300 shares    $100.10 - 500 shares
$99.90  - 200 shares    $100.15 - 200 shares
```

### 3. Automatic Iceberg Refills
```
Event Queue:
t=0.000: Submit iceberg (100 visible)
t=0.100: Market buy 100 → iceberg filled
t=0.101: REFILL_EVENT → add next 100 shares
t=0.200: Market buy 100 → iceberg filled again
t=0.201: REFILL_EVENT → add next 100 shares
... continues until peak_quantity exhausted
```

### Example Output:
```
BASIC SIMULATION EXAMPLE
Initial state: MarketSimulator(time=0.00, orders=0, trades=0, volume=0)
After initial orders:
  Best bid: $99.95
  Best ask: $100.05
  Spread: $0.10

ICEBERG ORDER EXAMPLE
Submitting ICEBERG sell order
  Peak: 10,000 shares | Visible tip: 100 shares | Price: $100.00
After buying 500 shares:
  Trades: 5
  Volume: 500
  Iceberg refills: 4  ← KEY METRIC!
```

## Features You Can Extract

From the snapshots and order book states:

```python
# Already available via sim.snapshots
snapshot = {
    'timestamp': 5.0,
    'best_bid': 99.95,
    'best_ask': 100.05,
    'spread': 0.10,
    'bid_depth': [(99.95, 100, 1), (99.90, 200, 1), ...],
    'ask_depth': [(100.05, 300, 2), (100.10, 150, 1), ...],
    'total_bid_quantity': 500,
    'total_ask_quantity': 650,
}

# You'll compute from these:
- Resilience: volume / price_change
- Refill rate: new_quantity / time_elapsed
- Persistence: time_at_level
- Imbalance: (bid_qty - ask_qty) / (bid_qty + ask_qty)
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│         MarketSimulator (orchestrator)          │
├─────────────────────────────────────────────────┤
│  - Configuration                                │
│  - Statistics                                   │
│  - History (orders, trades, snapshots)          │
│  - Callbacks (monitoring hooks)                 │
└────────┬──────────────────────┬─────────────────┘
         │                      │
         ▼                      ▼
┌──────────────────┐    ┌──────────────────┐
│   Event Queue    │    │  Matching Engine │
│                  │    │                  │
│ - Priority heap  │◄───┤ - Order routing  │
│ - Event types    │    │ - Trade gen      │
│ - Scheduling     │    │ - Iceberg refill │
└──────────────────┘    └────────┬─────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │   Order Book     │
                        │                  │
                        │ - Price levels   │
                        │ - FIFO queues    │
                        │ - Depth queries  │
                        └──────────────────┘
```

## What Makes This Special for Iceberg Detection

1. **Ground Truth Labels**: You inject icebergs, so you know exactly where they are
2. **Realistic Microstructure**: Price-time priority, proper matching
3. **Automatic Refills**: Iceberg behavior is authentic
4. **Full History**: Every event is recorded for feature extraction
5. **Deterministic**: Random seed makes experiments reproducible