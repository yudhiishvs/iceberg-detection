"""
Server architecture: run the order book for one minute with buy and sell agents
submitting limit orders at a desired price with Gaussian randomness.

Orders and trades are logged to the console and a log file for the full run.
"""

import logging
import random
from pathlib import Path

from core import (
    MarketSimulator,
    SimulationConfig,
    EventType,
    PeriodicEvent,
)
from core.order import OrderSide, NaiveIcebergOrder
from agents import BuyAgent, SellAgent
from agents.iceberg import inject_iceberg_orders, ICEBERG_BUY_ORDER_ID, ICEBERG_SELL_ORDER_ID


# --- Config (price will later follow market; for now fixed) ---
DURATION_SEC = 60.0  # Use 15.0 for a shorter run
TARGET_PRICE = 100.0
PRICE_STD = 0.05
ORDERS_PER_SECOND_PER_AGENT = 5
QUANTITY_MIN = 10
QUANTITY_MAX = 100
RANDOM_SEED = 42

# Iceberg injection: one buy + one sell over the run
ICEBERG_PEAK_QUANTITY = 15_000
ICEBERG_VISIBLE_QUANTITY = 25

# Log file next to this script (e.g. run_60s.log or run_15s.log)
LOG_DIR = Path(__file__).resolve().parent
LOG_FILENAME = LOG_DIR / f"logs/run_{int(DURATION_SEC)}s.log"


def setup_run_logging(log_filepath: Path):
    """Configure logging to console and file for the duration of the run."""
    log_filepath.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("new_run")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(message)s")
    # Console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # File
    fh = logging.FileHandler(log_filepath, mode="w", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def run_server():
    """Run the order book for one minute with buy and sell agents."""
    random.seed(RANDOM_SEED)

    logger = setup_run_logging(LOG_FILENAME)
    logger.info("=== Order book server run (duration=%.1fs, target_price=%.2f) ===\n", DURATION_SEC, TARGET_PRICE)

    config = SimulationConfig(
        initial_mid_price=TARGET_PRICE,
        tick_size=0.01,
        start_time=0.0,
        end_time=DURATION_SEC,
        snapshot_interval=1.0,
        random_seed=RANDOM_SEED,
    )
    sim = MarketSimulator(config)

    # Log every order and trade for the full run; label iceberg orders as ICEBERG_BUY / ICEBERG_SELL
    def on_order(order, result):
        t = order.timestamp
        side = order.side.value
        oid = order.order_id  # ICEBERG_BUY / ICEBERG_SELL for iceberg orders
        qty = order.quantity
        price = getattr(order, "price", None)
        price_str = f" @ ${price:.2f}" if price is not None else ""
        acc = "accepted" if result.accepted else f"rejected({result.rejection_reason or '?'})"
        filled = result.filled_quantity
        peak = getattr(order, "peak_quantity", None)
        peak_str = " peak=%s" % peak if peak is not None else ""
        logger.info("[t=%7.2fs] ORDER  %s id=%s qty=%s%s -> %s (filled=%s)%s", t, side, oid, qty, price_str, acc, filled, peak_str)

    def on_trade(trade):
        t = trade.timestamp
        iceberg_tag = ""
        if trade.buy_order_id in (ICEBERG_BUY_ORDER_ID, ICEBERG_SELL_ORDER_ID) or trade.sell_order_id in (ICEBERG_BUY_ORDER_ID, ICEBERG_SELL_ORDER_ID):
            iceberg_tag = " [iceberg]"
        logger.info("[t=%7.2fs] TRADE  %s qty=%s @ $%.2f (aggressor=%s)%s", t, trade.trade_id, trade.quantity, trade.price, trade.aggressor_side.value, iceberg_tag)

    sim.on_order(on_order)
    sim.on_trade(on_trade)

    # One buy iceberg over the run; inject at t=15s so the market has orders first
    def iceberg_inject():
        inject_iceberg_orders(
            sim,
            current_time=sim.current_time,
            target_price=TARGET_PRICE,
            peak_quantity=ICEBERG_PEAK_QUANTITY,
            visible_quantity=ICEBERG_VISIBLE_QUANTITY,
        )

    sim.event_queue.schedule(
        timestamp=15.0,
        event_type=EventType.AGENT_ACTION,
        callback=iceberg_inject,
    )

    # Agents: orders at Gaussian(desired_price, PRICE_STD)
    buy_agent = BuyAgent(
        agent_id="buy_agent",
        target_price=TARGET_PRICE,
        price_std=PRICE_STD,
        quantity_min=QUANTITY_MIN,
        quantity_max=QUANTITY_MAX,
    )
    sell_agent = SellAgent(
        agent_id="sell_agent",
        target_price=TARGET_PRICE,
        price_std=PRICE_STD,
        quantity_min=QUANTITY_MIN,
        quantity_max=QUANTITY_MAX,
    )

    interval = 1.0 / ORDERS_PER_SECOND_PER_AGENT

    def buy_agent_tick():
        if sim.current_time >= DURATION_SEC:
            return
        buy_agent.submit_orders(sim, current_time=sim.current_time)

    def sell_agent_tick():
        if sim.current_time >= DURATION_SEC:
            return
        sell_agent.submit_orders(sim, current_time=sim.current_time)

    # Multiple orders per second: each agent fires every `interval` seconds
    PeriodicEvent(
        event_queue=sim.event_queue,
        interval=interval,
        event_type=EventType.AGENT_ACTION,
        callback=buy_agent_tick,
        start_time=interval,
    )
    PeriodicEvent(
        event_queue=sim.event_queue,
        interval=interval,
        event_type=EventType.AGENT_ACTION,
        callback=sell_agent_tick,
        start_time=interval,
    )

    logger.info("Running for %.1fs (target $%.2f, ~%s orders/s per side)\n", DURATION_SEC, TARGET_PRICE, ORDERS_PER_SECOND_PER_AGENT)
    sim.run(until_time=DURATION_SEC)

    logger.info("\n=== Done === orders=%s trades=%s volume=%s | best_bid=%s best_ask=%s", sim.stats.total_orders, sim.stats.total_trades, sim.stats.total_volume, sim.order_book.best_bid, sim.order_book.best_ask)
    logger.info("Log file: %s", LOG_FILENAME)
    return sim


if __name__ == "__main__":
    run_server()
