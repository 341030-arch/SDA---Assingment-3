"""
producer_script.py
-------------------
Generates realistic mock stock-tick data using Faker and streams it
continuously to a Kafka topic.

Run from the project root with:
    python producer/producer_script.py
"""

import os
import json
import time
import random
import logging
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
# Read from environment variables so the same code works unchanged whether
# it's run directly on your machine (defaults to localhost) or inside the
# Docker Compose network (compose injects KAFKA_BROKER=kafka:9092).
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "sample_topic")  # rename to
                                      # "stock-ticks" if you want to match
                                      # the full 5-pipeline design later
MIN_DELAY_SEC = 0.3                  # slight delay between messages
MAX_DELAY_SEC = 1.0

# A fixed universe of symbols so the consumer can build meaningful
# per-symbol history (a brand-new random symbol every tick would make
# moving-average / % change alerts meaningless).
SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "ITC"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("producer")

fake = Faker()

# In-memory "last price" per symbol so each new tick is a realistic small
# random-walk step instead of a fully random number. This is what allows
# the consumer's "price moved >3% in a short window" alert to actually
# fire in a believable way during a demo.
last_price = {symbol: round(random.uniform(500, 3500), 2) for symbol in SYMBOLS}


def generate_tick() -> dict:
    """Builds a single mock stock-tick JSON record."""
    symbol = random.choice(SYMBOLS)

    # Random walk: usually a small drift, occasionally a bigger jump so
    # alert logic has something interesting to catch.
    if random.random() < 0.05:          # ~5% chance of a sharp move
        pct_move = random.uniform(-0.05, 0.05)
    else:
        pct_move = random.uniform(-0.006, 0.006)

    new_price = round(last_price[symbol] * (1 + pct_move), 2)
    new_price = max(new_price, 1.0)     # guard against negative/zero price
    last_price[symbol] = new_price

    bid = round(new_price - random.uniform(0.05, 0.5), 2)
    ask = round(new_price + random.uniform(0.05, 0.5), 2)

    tick = {
        "event_id": fake.uuid4(),
        "symbol": symbol,
        "price": new_price,
        "bid": bid,
        "ask": ask,
        "volume": random.randint(1, 5000),
        "trader_id": fake.uuid4(),
        "exchange": random.choice(["NSE", "BSE"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return tick


def build_producer() -> KafkaProducer:
    """Creates and returns a configured KafkaProducer with retry handling."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=5,
            linger_ms=10,
        )
        logger.info("Connected to Kafka broker at %s", KAFKA_BROKER)
        return producer
    except NoBrokersAvailable:
        logger.error(
            "Could not reach Kafka broker at %s. "
            "Is your Kafka server running?", KAFKA_BROKER
        )
        raise


def main():
    producer = build_producer()
    logger.info("Starting tick stream -> topic '%s'. Press Ctrl+C to stop.", TOPIC_NAME)

    try:
        while True:
            tick = generate_tick()
            try:
                # Use the symbol as the message key so Kafka can (optionally)
                # partition by symbol later for ordered per-symbol processing.
                future = producer.send(TOPIC_NAME, key=tick["symbol"], value=tick)
                future.get(timeout=10)  # block briefly to catch send errors
                logger.info("Sent: %s", tick)
            except KafkaError as e:
                logger.error("Failed to send message: %s", e)

            time.sleep(random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC))

    except KeyboardInterrupt:
        logger.info("Stopping producer (Ctrl+C received)...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    main()
