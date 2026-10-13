"""
consumer_script.py
-------------------
Consumes stock-tick JSON messages from Kafka, persists every message to
MongoDB, and runs two real-time alert rules per symbol:

  1. Volatility Alert  - current price deviates too far from the recent
                          rolling average.
  2. Price Spike Alert - price has moved more than +/-3% within the last
                          60 seconds.

Run from the project root with:
    python consumer/consumer_script.py
"""

import os
import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
# Read from environment variables so the same code works unchanged whether
# it's run directly on your machine (defaults to localhost) or inside the
# Docker Compose network (compose injects the service hostnames below).
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "sample_topic")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.environ.get("MONGO_DB_NAME", "streaming_db")
RAW_COLLECTION = "sample_data"      # every tick lands here
ALERTS_COLLECTION = "alerts"        # only triggered alerts land here

# ---- Alert thresholds (tune these to taste) -------------------------------
MOVING_AVG_WINDOW = 5           # number of recent ticks used for the average
VOLATILITY_THRESHOLD_PCT = 2.0  # % deviation from moving average -> alert
PRICE_SPIKE_THRESHOLD_PCT = 3.0 # % move within the lookback window -> alert
SPIKE_LOOKBACK_SECONDS = 60     # "1 minute" window for the spike rule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("consumer")

# Per-symbol rolling history of (timestamp, price) tuples, used by both
# alert rules. deque with maxlen keeps memory bounded automatically.
price_history = defaultdict(lambda: deque(maxlen=200))


# --------------------------------------------------------------------------
# INFRASTRUCTURE SETUP (Kafka + MongoDB), with error handling
# --------------------------------------------------------------------------
def build_consumer() -> KafkaConsumer:
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: v.decode("utf-8"),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="latest",   # only new messages once running
            enable_auto_commit=True,
            group_id="sample-consumer-group",
        )
        logger.info("Connected to Kafka broker at %s, subscribed to '%s'",
                    KAFKA_BROKER, TOPIC_NAME)
        return consumer
    except NoBrokersAvailable:
        logger.error("Could not reach Kafka broker at %s. Is Kafka running?", KAFKA_BROKER)
        raise


def build_mongo_collections():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")  # forces a real connection check
        db = client[DB_NAME]
        logger.info("Connected to MongoDB at %s (db: %s)", MONGO_URI, DB_NAME)
        return db[RAW_COLLECTION], db[ALERTS_COLLECTION]
    except ServerSelectionTimeoutError:
        logger.error(
            "Could not connect to MongoDB at %s. "
            "Is your local MongoDB server running?", MONGO_URI
        )
        raise


# --------------------------------------------------------------------------
# ALERT LOGIC
# --------------------------------------------------------------------------
def check_volatility_alert(symbol: str, current_price: float) -> dict | None:
    """
    Rule 1: Volatility Alert
    ------------------------
    Compares the current price against the simple moving average of the
    last MOVING_AVG_WINDOW ticks for that symbol. If the current price
    deviates from that average by more than VOLATILITY_THRESHOLD_PCT%,
    it means the symbol is moving unusually fast relative to its own
    recent behaviour -> raise an alert.

    This mirrors "moving_avg_consumer.py" from the original design, but
    folded into a single consumer for simplicity.
    """
    history = price_history[symbol]
    if len(history) < MOVING_AVG_WINDOW:
        return None  # not enough data yet to compute a meaningful average

    recent_prices = [p for _, p in list(history)[-MOVING_AVG_WINDOW:]]
    moving_avg = sum(recent_prices) / len(recent_prices)
    if moving_avg == 0:
        return None

    deviation_pct = abs(current_price - moving_avg) / moving_avg * 100
    if deviation_pct > VOLATILITY_THRESHOLD_PCT:
        return {
            "alert_type": "VOLATILITY_ALERT",
            "symbol": symbol,
            "current_price": current_price,
            "moving_average": round(moving_avg, 2),
            "deviation_pct": round(deviation_pct, 2),
            "threshold_pct": VOLATILITY_THRESHOLD_PCT,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
    return None


def check_price_spike_alert(symbol: str, current_price: float, current_time: datetime) -> dict | None:
    """
    Rule 2: Price Spike Alert
    --------------------------
    Looks back SPIKE_LOOKBACK_SECONDS (default 60s) into that symbol's
    history, finds the oldest price still inside that window, and checks
    whether the price has moved more than PRICE_SPIKE_THRESHOLD_PCT% (3%)
    since then. This mirrors "price_alert_consumer.py" from the original
    design ("price moves > 3% in 1 minute").
    """
    history = price_history[symbol]
    cutoff = current_time.timestamp() - SPIKE_LOOKBACK_SECONDS

    # Find the earliest price sample still within the lookback window.
    reference_price = None
    for ts, price in history:
        if ts.timestamp() >= cutoff:
            reference_price = price
            break

    if reference_price is None or reference_price == 0:
        return None

    change_pct = (current_price - reference_price) / reference_price * 100
    if abs(change_pct) > PRICE_SPIKE_THRESHOLD_PCT:
        return {
            "alert_type": "PRICE_SPIKE_ALERT",
            "symbol": symbol,
            "current_price": current_price,
            "reference_price": reference_price,
            "change_pct": round(change_pct, 2),
            "window_seconds": SPIKE_LOOKBACK_SECONDS,
            "threshold_pct": PRICE_SPIKE_THRESHOLD_PCT,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
    return None


def evaluate_alerts(tick: dict, alerts_collection) -> None:
    """Runs both alert rules for an incoming tick and persists any that fire."""
    symbol = tick["symbol"]
    price = float(tick["price"])
    now = datetime.now(timezone.utc)

    # Update rolling history BEFORE evaluating, so the current tick is
    # eligible to be its own "most recent" data point for the next check.
    price_history[symbol].append((now, price))

    for alert in (
        check_volatility_alert(symbol, price),
        check_price_spike_alert(symbol, price, now),
    ):
        if alert:
            try:
                alerts_collection.insert_one(alert)
            except PyMongoError as e:
                logger.error("Failed to persist alert to MongoDB: %s", e)
            logger.warning("ALERT | %s | %s", alert["alert_type"], alert)


# --------------------------------------------------------------------------
# MAIN CONSUME LOOP
# --------------------------------------------------------------------------
def main():
    consumer = build_consumer()
    raw_collection, alerts_collection = build_mongo_collections()

    logger.info("Listening for messages on '%s'. Press Ctrl+C to stop.", TOPIC_NAME)

    try:
        for message in consumer:
            raw_value = message.value

            # --- Parse JSON safely ---
            try:
                tick = json.loads(raw_value)
            except (json.JSONDecodeError, TypeError) as e:
                logger.error("Skipping unparseable message (%s): %r", e, raw_value)
                continue

            # --- Persist raw tick to MongoDB safely ---
            try:
                raw_collection.insert_one(dict(tick))  # copy: insert_one mutates with _id
            except PyMongoError as e:
                logger.error("Failed to insert document into MongoDB: %s", e)
                continue  # still fine to run alert logic even if the DB write failed

            logger.info("Stored: %s", tick)

            # --- Run alert/trigger logic ---
            try:
                evaluate_alerts(tick, alerts_collection)
            except (KeyError, ValueError, TypeError) as e:
                logger.error("Alert evaluation failed for tick %s: %s", tick, e)

    except KeyboardInterrupt:
        logger.info("Stopping consumer (Ctrl+C received)...")
    except KafkaError as e:
        logger.error("Kafka error while consuming: %s", e)
    finally:
        consumer.close()
        logger.info("Consumer closed cleanly.")


if __name__ == "__main__":
    main()
