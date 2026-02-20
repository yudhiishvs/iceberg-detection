"""
Example usage of the core simulation engine.

This script demonstrates:
1. Creating a simulator
2. Submitting regular limit orders
3. Submitting iceberg orders
4. Running the simulation
5. Analyzing results
"""

import random
from core import (
    create_default_simulator,
    LimitOrder,
    MarketOrder,
    NaiveIcebergOrder,
    OrderSide,
    OrderStatus,
)


def example_basic_simulation():
    """Basic example: submit some orders and run simulation"""
    print("=" * 60)
    print("BASIC SIMULATION EXAMPLE")
    print("=" * 60)
    
    # Create simulator (1 minute simulation)
    sim = create_default_simulator(duration=60.0, random_seed=42)
    
    print(f"\nInitial state: {sim}")
    print(f"Order book: {sim.order_book}")
    
    # Submit some initial limit orders to establish a market
    orders = [
        # Buy side
        LimitOrder("L001", 0, "trader1", OrderSide.BUY, price=99.95, quantity=100),
        LimitOrder("L002", 0, "trader1", OrderSide.BUY, price=99.90, quantity=200),
        LimitOrder("L003", 0, "trader2", OrderSide.BUY, price=99.85, quantity=150),
        
        # Sell side
        LimitOrder("L004", 0, "trader3", OrderSide.SELL, price=100.05, quantity=100),
        LimitOrder("L005", 0, "trader3", OrderSide.SELL, price=100.10, quantity=200),
        LimitOrder("L006", 0, "trader4", OrderSide.SELL, price=100.15, quantity=150),
    ]
    
    for order in orders:
        sim.submit_order(order)
    
    # Process these orders
    sim.run_steps(len(orders))
    
    print(f"\nAfter initial orders:")
    print(f"Order book: {sim.order_book}")
    print(f"Best bid: ${sim.order_book.best_bid:.2f}")
    print(f"Best ask: ${sim.order_book.best_ask:.2f}")
    print(f"Spread: ${sim.order_book.spread:.2f}")
    
    # Submit a market order that will execute
    print("\n--- Submitting market buy order for 150 shares ---")
    market_order = MarketOrder("M001", 0, "trader5", OrderSide.BUY, quantity=150)
    sim.submit_order(market_order)
    sim.run_steps(1)
    
    print(f"\nAfter market order:")
    print(f"Order book: {sim.order_book}")
    print(f"Trades executed: {sim.stats.total_trades}")
    print(f"Volume traded: {sim.stats.total_volume}")
    
    # Show trade details
    for trade in sim.trade_history:
        print(f"  Trade {trade.trade_id}: {trade.quantity} @ ${trade.price:.2f}")
    
    return sim


def example_iceberg_order():
    """Example with iceberg orders"""
    print("\n" + "=" * 60)
    print("ICEBERG ORDER EXAMPLE")
    print("=" * 60)
    
    sim = create_default_simulator(duration=60.0, random_seed=42)
    
    # Set up order book
    sim.submit_order(LimitOrder("L001", 0, "mm1", OrderSide.BUY, price=99.90, quantity=500))
    sim.submit_order(LimitOrder("L002", 0, "mm1", OrderSide.SELL, price=100.10, quantity=500))
    sim.run_steps(2)
    
    print(f"\nInitial book:")
    print(f"Best bid: ${sim.order_book.best_bid:.2f} (500 shares)")
    print(f"Best ask: ${sim.order_book.best_ask:.2f} (500 shares)")
    
    # Submit an iceberg order on the sell side
    # Total size: 10,000 shares
    # Visible tip: 100 shares at a time
    print("\n--- Submitting ICEBERG sell order ---")
    print("Peak: 10,000 shares | Visible tip: 100 shares | Price: $100.00")
    
    iceberg = NaiveIcebergOrder(
        order_id="ICE001",
        timestamp=0,
        trader_id="whale1",
        side=OrderSide.SELL,
        price=100.00,
        peak_quantity=10000,
        visible_quantity=100,
        quantity=100
    )
    
    sim.submit_order(iceberg)
    sim.run_steps(1)
    
    print(f"\nBook after iceberg submission:")
    print(f"Best ask: ${sim.order_book.best_ask:.2f}")
    ask_depth = sim.order_book.get_depth(OrderSide.SELL, levels=5)
    for price, qty, orders in ask_depth:
        print(f"  ${price:.2f}: {qty} shares ({orders} orders)")
    
    # Now send aggressive buy orders to "discover" the iceberg
    print("\n--- Sending aggressive buy orders to hit the iceberg ---")
    
    for i in range(5):
        buy_order = MarketOrder(
            order_id=f"M{i:03d}",
            timestamp=0,
            trader_id=f"buyer{i}",
            side=OrderSide.BUY,
            quantity=100
        )
        sim.submit_order(buy_order, delay=i * 0.1)
    
    # Run the simulation
    sim.run(until_time=1.0)
    
    print(f"\nAfter buying 500 shares:")
    print(f"Trades: {sim.stats.total_trades}")
    print(f"Volume: {sim.stats.total_volume}")
    print(f"Iceberg refills: {sim.stats.iceberg_refills}")
    print(f"Iceberg filled: {iceberg.filled_quantity} / {iceberg.peak_quantity}")
    print(f"Iceberg hidden remaining: {iceberg.hidden_remaining}")
    
    # Check if iceberg is still on the book
    ask_depth = sim.order_book.get_depth(OrderSide.SELL, levels=3)
    print(f"\nCurrent ask depth:")
    for price, qty, orders in ask_depth:
        print(f"  ${price:.2f}: {qty} shares ({orders} orders)")
    
    return sim


def example_with_callbacks():
    """Example using callbacks to monitor the simulation"""
    print("\n" + "=" * 60)
    print("CALLBACK MONITORING EXAMPLE")
    print("=" * 60)
    
    sim = create_default_simulator(duration=10.0, random_seed=42)
    
    # Set up callbacks
    trade_count = [0]  # Use list to allow modification in closure
    
    def on_trade(trade):
        trade_count[0] += 1
        print(f"TRADE #{trade_count[0]}: {trade.quantity} @ ${trade.price:.2f} "
              f"({trade.aggressor_side.value} aggressor)")
    
    def on_snapshot(snapshot):
        if snapshot['timestamp'] % 5 == 0:  # Print every 5 seconds
            bid = f"${snapshot['best_bid']:.2f}" if snapshot['best_bid'] is not None else "N/A"
            ask = f"${snapshot['best_ask']:.2f}" if snapshot['best_ask'] is not None else "N/A"
            spread = f"${snapshot['spread']:.2f}" if snapshot['spread'] is not None else "N/A"
            
            print(f"[t={snapshot['timestamp']:.0f}s] "
                  f"Bid: {bid} | "
                  f"Ask: {ask} | "
                  f"Spread: {spread}")
    
    sim.on_trade(on_trade)
    sim.on_snapshot(on_snapshot)
    
    # Create initial market
    for i in range(10):
        price_offset = (i - 5) * 0.05
        if i < 5:
            # Buy orders
            order = LimitOrder(
                f"L{i:03d}", 0, f"trader{i}", OrderSide.BUY,
                price=100.00 + price_offset,
                quantity=random.randint(50, 200)
            )
        else:
            # Sell orders
            order = LimitOrder(
                f"L{i:03d}", 0, f"trader{i}", OrderSide.SELL,
                price=100.00 - price_offset,
                quantity=random.randint(50, 200)
            )
        sim.submit_order(order, delay=i * 0.1)
    
    # Add some market orders
    for i in range(5):
        side = OrderSide.BUY if random.random() > 0.5 else OrderSide.SELL
        order = MarketOrder(
            f"M{i:03d}", 0, f"trader{10+i}", side,
            quantity=random.randint(20, 100)
        )
        sim.submit_order(order, delay=2.0 + i * 1.0)
    
    print("\nRunning simulation...\n")
    sim.run()
    
    print(f"\n--- Simulation Complete ---")
    print(f"Final stats: {sim.stats.total_trades} trades, "
          f"{sim.stats.total_volume} volume")
    
    return sim


def main():
    """Run all examples"""
    # Run examples
    sim1 = example_basic_simulation()
    sim2 = example_iceberg_order()
    sim3 = example_with_callbacks()
    
    print("\n" + "=" * 60)
    print("ALL EXAMPLES COMPLETE")
    print("=" * 60)
    print("\nYou can now:")
    print("1. Examine sim.order_history for all orders")
    print("2. Examine sim.trade_history for all trades")
    print("3. Examine sim.snapshots for order book states over time")
    print("4. Use this data to extract features for ML training")


if __name__ == "__main__":
    main()