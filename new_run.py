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
from core.order import OrderSide
from agents import BuyAgent, SellAgent


# --- Config (price will later follow market; for now fixed) ---
DURATION_SEC = 60.0  # Use 15.0 for a shorter run
TARGET_PRICE = 100.0
PRICE_STD = 0.05
ORDERS_PER_SECOND_PER_AGENT = 5
QUANTITY_MIN = 10
QUANTITY_MAX = 100
RANDOM_SEED = 42

# Log file next to this script (e.g. run_60s.log or run_15s.log)
LOG_DIR = Path(__file__).resolve().parent
LOG_FILENAME = LOG_DIR / f"logs/run_{int(DURATION_SEC)}s.log"


def setup_run_logging(log_filepath: Path):
    """Configure logging to console and file for the duration of the run."""
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

    # Log every order and trade for the full run
    def on_order(order, result):
        t = order.timestamp
        side = order.side.value
        oid = order.order_id
        qty = order.quantity
        price = getattr(order, "price", None)
        price_str = f" @ ${price:.2f}" if price is not None else ""
        acc = "accepted" if result.accepted else f"rejected({result.rejection_reason or '?'})"
        filled = result.filled_quantity
        logger.info("[t=%7.2fs] ORDER  %s id=%s qty=%s%s -> %s (filled=%s)", t, side, oid, qty, price_str, acc, filled)

    def on_trade(trade):
        t = trade.timestamp
        logger.info("[t=%7.2fs] TRADE  %s qty=%s @ $%.2f (aggressor=%s)", t, trade.trade_id, trade.quantity, trade.price, trade.aggressor_side.value)

    sim.on_order(on_order)
    sim.on_trade(on_trade)

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
