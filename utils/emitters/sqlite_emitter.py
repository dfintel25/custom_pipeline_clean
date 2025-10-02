"""
sqlite_emitter.py

Emit messages into a SQLite database.

A SQLite emitter writes each message into a relational table:
- SQLite ships with Python (no install required).
- Provides simple queries and joins via SQL.
- Suitable for local persistence and small projects.

Use this when you want streaming data to land in a local, portable database
or when you want streaming data in a relational store for later analysis.

SQLite: 
INTEGER PRIMARY KEY piggybacks on the rowid and acts like an auto-incrementing key; 
AUTOINCREMENT is not necessary and often discouraged.
"""

import sqlite3
import pathlib
from typing import Mapping, Any
import pandas as pd

from utils.utils_logger import logger

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS streamed_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT,
    author TEXT,
    timestamp TEXT,
    category TEXT,
    sentiment REAL,
    keyword_mentioned TEXT,
    message_length INTEGER, 
    department_ID TEXT,
    job_classification TEXT
);
"""


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(_TABLE_SQL)
    conn.commit()


def emit_message(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """
    Insert one message (dict-like) into SQLite.

    Args:
        message:  Dict-like payload with expected keys.
        db_path:  Path to the SQLite file.

    Returns:
        True on success, False on failure.
    """
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            _ensure_table(conn)
            conn.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment,
                    keyword_mentioned, message_length, department_ID, job_classification
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.get("message"),
                    message.get("author"),
                    message.get("timestamp"),
                    message.get("category"),
                    float(message.get("sentiment", 0.0)),
                    message.get("keyword_mentioned"),
                    int(message.get("message_length", 0)),
                    message.get("department_ID"),
                    message.get("job_classification"),
                ),
            )
            conn.commit()
        logger.debug(f"[sqlite_emitter] inserted message into {db_path}")
        return True
    except Exception as e:
        logger.error(f"[sqlite_emitter] failed to insert into {db_path}: {e}")
        return False

def append_to_csv(message: dict, csv_path: pathlib.Path):
    df = pd.DataFrame([message])
    if csv_path.exists():
        df.to_csv(csv_path, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_path, mode='w', header=True, index=False)