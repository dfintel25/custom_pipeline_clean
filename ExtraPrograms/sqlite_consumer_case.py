import os
import pathlib
import sqlite3

import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database with all message fields.
    """
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
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
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize SQLite DB at {db_path}: {e}")


#####################################
# Insert a Message
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database.
    """
    logger.info(f"Inserting message: {message}")
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, 
                    message_length, hour_of_day, cash_type, Time_of_Day, Weekday, 
                    Month_name, Date, Time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")


#####################################
# Delete a Message
#####################################

def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message: {e}")


#####################################
# Main for Testing
#####################################

def main():
    logger.info("Starting SQLite consumer testing.")

    DATA_PATH: pathlib.Path = config.get_base_data_path()
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize DB
    init_db(TEST_DB_PATH)

    # Example new message
    test_message = {
        "message": "Purchased Latte for 38.7 via card",
        "author": "CafeUser",
        "timestamp": "2025-01-29 14:35:20",
        "category": "beverage",
        "sentiment": 0.5,
        "keyword_mentioned": "Latte",
        "message_length": 25,
        "hour_of_day": 10,
        "cash_type": "card",
        "Time_of_Day": "Morning",
        "Weekday": "Fri",
        "Month_name": "Mar",
        "Date": "01/03/2024",
        "Time": "15:50.5",
    }

    insert_message(test_message, TEST_DB_PATH)

    # Optionally delete the test message
    try:
        with sqlite3.connect(TEST_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                delete_message(row[0], TEST_DB_PATH)
    except Exception as e:
        logger.error(f"Failed to retrieve/delete test message: {e}")

    logger.info("SQLite consumer testing finished.")


if __name__ == "__main__":
    main()