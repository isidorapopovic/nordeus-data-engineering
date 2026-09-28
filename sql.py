import sqlite3
import json
from pathlib import Path
import pandas as pd

DB_PATH = Path("nordeus.db")
JSONL_PATH = Path("events.jsonl")

def show(title, query, conn):
    print(f"\n{title}")
    print(pd.read_sql_query(query, conn))


with sqlite3.connect(DB_PATH) as conn:
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_events (
            id INTEGER,
            timestamp INTEGER,
            event_type TEXT,
            user_id TEXT,
            country TEXT,
            device_os TEXT,
            username TEXT
        )
    """)

    with JSONL_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            raw = json.loads(line)
            if not isinstance(raw, dict):
                continue

            user = raw.get("user") or {}
            cursor.execute(
                """
                INSERT INTO bronze_events (
                    id, timestamp, event_type, user_id, country, device_os, username
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw.get("id"),
                    raw.get("timestamp"),
                    raw.get("event_type"),
                    raw.get("user_id"),
                    user.get("country"),
                    user.get("device_os"),
                    user.get("username"),
                ),
            )

    conn.commit()

    print("BRONZE")
    show("Bronze sample", "SELECT * FROM bronze_events LIMIT 5", conn)
    print(pd.read_sql_query("SELECT COUNT(*) AS bronze_rows FROM bronze_events", conn))

    cursor.execute("DROP TABLE IF EXISTS silver_events")
    cursor.execute("""
        CREATE TABLE silver_events AS
        SELECT DISTINCT
            id AS event_id,
            timestamp,
            LOWER(TRIM(event_type)) AS event_type,
            user_id,
            UPPER(TRIM(country)) AS country,
            LOWER(TRIM(device_os)) AS device_os,
            TRIM(username) AS username
        FROM bronze_events
        WHERE id IS NOT NULL
          AND user_id IS NOT NULL
          AND event_type IS NOT NULL
    """)

    conn.commit()

    show("SILVER", "SELECT * FROM silver_events LIMIT 10", conn)
    print(pd.read_sql_query("SELECT COUNT(*) AS silver_rows FROM silver_events", conn))

    print("\nEvent types:")
    print(pd.read_sql_query("""
        SELECT event_type, COUNT(*) AS count
        FROM silver_events
        GROUP BY event_type
        ORDER BY count DESC
    """, conn))

    print("\nCountries:")
    print(pd.read_sql_query("""
        SELECT country, COUNT(*) AS count
        FROM silver_events
        GROUP BY country
        ORDER BY count DESC
    """, conn))

    print("\nOperating systems:")
    print(pd.read_sql_query("""
        SELECT device_os, COUNT(*) AS count
        FROM silver_events
        GROUP BY device_os
        ORDER BY count DESC
    """, conn))