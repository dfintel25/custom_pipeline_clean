import csv
import pathlib
import sys
import time
from datetime import datetime
from typing import Mapping, Any

from kafka import KafkaProducer

import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger
from utils.emitters import file_emitter, kafka_emitter, sqlite_emitter, duckdb_emitter


#####################################
# CSV Reader Generator
#####################################

def generate_messages_from_csv(csv_path: pathlib.Path):
    """Yield one message dict per row from CSV file."""
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert fields to appropriate types
            try:
                message_length = len(str(row.get("coffee_name", "")))
                money = float(row.get("money", 0.0))
                hour_of_day = int(row.get("hour_of_day", 0))
            except Exception as e:
                logger.warning(f"Skipping row due to conversion error: {e}")
                continue

            # Construct message dict
            yield {
                "message": f"Purchased {row.get('coffee_name')} for {money} via {row.get('cash_type')}",
                "author": "CafeUser",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "category": "beverage",
                "sentiment": 0.5,  # optional static or computed
                "keyword_mentioned": row.get("coffee_name", "other"),
                "message_length": message_length,
                "hour_of_day": hour_of_day,
                "cash_type": row.get("cash_type"),
                "Time_of_Day": row.get("Time_of_Day"),
                "Weekday": row.get("Weekday"),
                "Month_name": row.get("Month_name"),
                "Date": row.get("Date"),
                "Time": row.get("Time"),
            }


#####################################
# Emitters (reuse your functions)
#####################################

def emit_to_file(message: Mapping[str, Any], *, path: pathlib.Path) -> bool:
    return file_emitter.emit_message(message, path=path)

def emit_to_kafka(message: Mapping[str, Any], *, producer: KafkaProducer, topic: str) -> bool:
    return kafka_emitter.emit_message(message, producer=producer, topic=topic)

def emit_to_sqlite(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    return sqlite_emitter.emit_message(message, db_path=db_path)

def emit_to_duckdb(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    return duckdb_emitter.emit_message(message, db_path=db_path)


#####################################
# Main
#####################################

def main() -> None:
    logger.info("Starting Producer from CSV.")
    logger.info("Use Ctrl+C to stop.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path() if hasattr(config, "get_sqlite_path") else pathlib.Path("data/buzz.sqlite")
        duckdb_path: pathlib.Path = config.get_duckdb_path() if hasattr(config, "get_duckdb_path") else pathlib.Path("data/buzz.duckdb")
        csv_input_path: pathlib.Path = pathlib.Path("data/coffee.csv")
    except Exception as e:
        logger.error(f"Failed to read environment variables: {e}")
        sys.exit(1)

    # Prepare file sink
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        live_data_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to prep live data file: {e}")
        sys.exit(2)

    # Setup Kafka (optional)
    producer = None
    try:
        if verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            logger.info(f"Kafka producer connected to {kafka_server}")
            try:
                create_kafka_topic(topic)
                logger.info(f"Kafka topic '{topic}' is ready.")
            except Exception as e:
                logger.warning(f"Topic create/verify failed ('{topic}'): {e}")
        else:
            logger.info("Kafka disabled for this run.")
    except Exception as e:
        logger.warning(f"Kafka setup failed: {e}")
        producer = None

    # Emit loop
    try:
        for message in generate_messages_from_csv(csv_input_path):
            logger.info(message)

            emit_to_file(message, path=live_data_path)

            if producer is not None:
                emit_to_kafka(message, producer=producer, topic=topic)
                producer.flush()
                logger.info("Flushed Kafka message")

            emit_to_sqlite(message, db_path=sqlite_path)
            emit_to_duckdb(message, db_path=duckdb_path)

            csv_path = pathlib.Path("data/survey_messages.csv")
            sqlite_emitter.append_to_csv(message, csv_path)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
                logger.info("Kafka producer closed.")
            except Exception:
                pass
        logger.info("Producer shutting down.")


if __name__ == "__main__":
    main()