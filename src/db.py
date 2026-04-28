import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2

logger = logging.getLogger(__name__)

_HISTORY_SQL_PATH = (
    Path(__file__).resolve().parent.parent / "sql" / "insert_into_identity_links_history.sql"
)
_LINKS_SQL_PATH = (
    Path(__file__).resolve().parent.parent / "sql" / "insert_into_identity_links.sql"
)

REQUIRED_ENV = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)


def get_connection():
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def insert_login_events(events, date):
    day_end = datetime.fromisoformat(date)
    day_start = day_end - timedelta(days=1)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM login_events WHERE ts >= %s AND ts < %s",
                (day_start, day_end),
            )

            rows = [
                (e["email"], e["ts"], e["ip"], e["event_json"], e["source_topic"])
                for e in events
            ]
            if rows:
                cur.executemany(
                    "INSERT INTO login_events (email, ts, ip, event_json, source_topic) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    rows,
                )

        conn.commit()
    except Exception:
        logger.exception("DB error during insert_login_events")
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_identity_link_history(date):
    day_end = datetime.fromisoformat(date)
    day_start = day_end - timedelta(days=1)

    insert_sql = _HISTORY_SQL_PATH.read_text()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM identity_links_history "
                "WHERE COALESCE(system_1_ts, system_2_ts, system_3_ts) >= %s "
                "AND COALESCE(system_1_ts, system_2_ts, system_3_ts) < %s",
                (day_start, day_end),
            )
            cur.execute(insert_sql, (day_start, day_end))
            inserted = cur.rowcount
        conn.commit()
        return inserted
    except Exception:
        logger.exception("DB error during insert_identity_link_history")
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_identity_link(date):
    day_end = datetime.fromisoformat(date)
    day_start = day_end - timedelta(days=1)

    merge_sql = _LINKS_SQL_PATH.read_text()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(merge_sql, (day_start, day_end))
            affected = cur.rowcount
        conn.commit()
        return affected
    except Exception:
        logger.exception("DB error during insert_identity_link")
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_identity_links(groups, date):
    yesterday = (datetime.fromisoformat(date) - timedelta(days=1)).date()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM identity_links WHERE last_updated = %s",
                (yesterday,),
            )

            rows = [(sorted(group), yesterday) for group in groups]
            if rows:
                cur.executemany(
                    "INSERT INTO identity_links (emails, last_updated) VALUES (%s, %s)",
                    rows,
                )

        conn.commit()
    except Exception:
        logger.exception("DB error during upsert_identity_links")
        conn.rollback()
        raise
    finally:
        conn.close()
