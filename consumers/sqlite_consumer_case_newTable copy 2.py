import json
import pathlib
import sqlite3
import time
from kafka import KafkaConsumer

import utils.utils_config as config
from utils.utils_logger import logger

COFFEE_TYPES = ["coffee", "espresso", "latte", "cappuccino", "americano", "cortado"]

#####################################
# Initialize SQLite Table
#####################################
def init_db(db_path: pathlib.Path, table_name: str = "coffee_sales"):
    logger.info(f"Initializing SQLite table '{table_name}' at {db_path}")
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Create table if it doesn't exist (original columns)
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER,
                    hour_of_day INTEGER,
                    cash_type TEXT,
                    Time_of_Day TEXT,
                    Weekday TEXT,
                    Month_name TEXT,
                    Date TEXT,
                    Time TEXT
                )
                """
            )
            conn.commit()

            # --- Add coffee_name column if it doesn't exist ---
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [col[1] for col in cursor.fetchall()]
            if "coffee_name" not in columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN coffee_name TEXT;")
                conn.commit()
                logger.info("Added column 'coffee_name' to coffee_sales")

        logger.info(f"Table '{table_name}' ready.")
    except Exception as e:
        logger.error(f"Failed to initialize table '{table_name}': {e}")
#####################################
# Extract coffee name from message
#####################################
def extract_coffee(message_text: str) -> str:
    if not message_text:
        return "Unknown"
    msg_lower = message_text.lower()
    for coffee in COFFEE_TYPES:
        if coffee in msg_lower:
            return coffee.capitalize()
    return "Other"

#####################################
# Insert Message into SQLite
#####################################
def insert_message(message: dict, db_path: pathlib.Path, table_name: str = "coffee_sales"):
    coffee_name = extract_coffee(message.get("message", ""))
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                INSERT INTO {table_name} (
                    message, author, timestamp, category, sentiment, keyword_mentioned,
                    message_length, hour_of_day, cash_type, Time_of_Day, Weekday,
                    Month_name, Date, Time, coffee_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.get("message"),
                    message.get("author"),
                    message.get("timestamp"),
                    message.get("category"),
                    message.get("sentiment"),
                    message.get("keyword_mentioned"),
                    message.get("message_length"),
                    message.get("hour_of_day"),
                    message.get("cash_type"),
                    message.get("Time_of_Day"),
                    message.get("Weekday"),
                    message.get("Month_name"),
                    message.get("Date"),
                    message.get("Time"),
                    coffee_name
                )
            )
            conn.commit()
        logger.info(f"Inserted message into '{table_name}': {message.get('message')} (coffee_name={coffee_name})")
    except Exception as e:
        logger.error(f"Failed to insert message: {e}")

#####################################
# Main Consumer Loop
#####################################
def main():
    logger.info("Starting continuous SQLite consumer for coffee messages.")

    buzz_sqlite: pathlib.Path = config.get_base_data_path() / "buzz.sqlite"
    TABLE_NAME = "coffee_sales"

    # Init table
    init_db(buzz_sqlite, table_name=TABLE_NAME)

    # Setup Kafka Consumer
    kafka_broker = config.get_kafka_broker_address()
    kafka_topic = config.get_kafka_topic()
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_broker],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="sqlite_coffee_consumer",
        value_deserializer=lambda m: m  # raw bytes, decode later
    )
    logger.info(f"Kafka consumer subscribed to topic '{kafka_topic}' on {kafka_broker}")

    try:
        for msg in consumer:
            if not msg.value:
                continue  # skip empty messages
            try:
                message = json.loads(msg.value.decode("utf-8"))
            except json.JSONDecodeError:
                logger.warning(f"Skipping invalid JSON message: {msg.value}")
                continue

            # Only process coffee messages
            if message.get("category") == "beverage" and any(
                coffee_word in message.get("message", "").lower()
                for coffee_word in COFFEE_TYPES
            ):
                logger.info(f"Coffee message found: {message}")
                insert_message(message, buzz_sqlite, table_name=TABLE_NAME)

            # Wait before consuming next message
            interval = config.get_message_interval_seconds_as_int()
            logger.info(f"MESSAGE_INTERVAL_SECONDS: {interval}")
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()