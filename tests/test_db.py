from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src import db
from src.db import (
    get_connection,
    insert_identity_link,
    insert_identity_link_history,
    insert_login_events,
    upsert_identity_links,
)


@pytest.fixture
def pg_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "identity_db")
    monkeypatch.setenv("POSTGRES_USER", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")


def test_get_connection_passes_env_to_psycopg2(pg_env):
    with patch.object(db, "psycopg2") as mock_pg:
        get_connection()
    mock_pg.connect.assert_called_once_with(
        host="localhost",
        port="5432",
        dbname="identity_db",
        user="postgres",
        password="secret",
    )


@pytest.mark.parametrize("missing", [
    "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
])
def test_get_connection_raises_when_env_missing(pg_env, monkeypatch, missing):
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(RuntimeError, match=missing):
        get_connection()


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cursor


def test_upsert_deletes_yesterday_then_inserts_groups(pg_env):
    conn, cursor = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        upsert_identity_links([["a@x.com", "b@x.com"], ["c@x.com", "d@x.com"]], "2026-04-28")

    yesterday = date(2026, 4, 27)

    cursor.execute.assert_called_once_with(
        "DELETE FROM identity_links WHERE last_updated = %s",
        (yesterday,),
    )
    cursor.executemany.assert_called_once()
    sql, rows = cursor.executemany.call_args.args
    assert "INSERT INTO identity_links" in sql
    assert rows == [
        (["a@x.com", "b@x.com"], yesterday),
        (["c@x.com", "d@x.com"], yesterday),
    ]
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_upsert_sorts_emails_within_each_group(pg_env):
    conn, cursor = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        upsert_identity_links([["b@x.com", "a@x.com", "c@x.com"]], "2026-04-28")

    rows = cursor.executemany.call_args.args[1]
    assert rows[0][0] == ["a@x.com", "b@x.com", "c@x.com"]


def test_upsert_with_no_groups_still_runs_delete(pg_env):
    conn, cursor = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        upsert_identity_links([], "2026-04-28")

    cursor.execute.assert_called_once()
    cursor.executemany.assert_not_called()
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_upsert_rolls_back_and_reraises_on_error(pg_env):
    conn, cursor = _mock_conn()
    cursor.execute.side_effect = RuntimeError("db down")

    with patch.object(db, "get_connection", return_value=conn):
        with pytest.raises(RuntimeError, match="db down"):
            upsert_identity_links([["a", "b"]], "2026-04-28")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_upsert_closes_connection_in_finally(pg_env):
    conn, _ = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        upsert_identity_links([], "2026-04-28")
    conn.close.assert_called_once()


def _event(email, ts, ip, source_topic="system_1", event_json=None):
    return {
        "email": email,
        "ts": ts,
        "ip": ip,
        "event_json": event_json or f'{{"email":"{email}","ts":"{ts}","ip":"{ip}"}}',
        "source_topic": source_topic,
    }


def test_insert_login_events_deletes_window_then_inserts(pg_env):
    conn, cursor = _mock_conn()
    events = [
        _event("a@x.com", "2026-04-27T10:00:00", "1.1.1.1", "system_1"),
        _event("b@x.com", "2026-04-27T10:05:00", "1.1.1.1", "system_2"),
    ]
    with patch.object(db, "get_connection", return_value=conn):
        insert_login_events(events, "2026-04-28")

    day_start = datetime(2026, 4, 27)
    day_end = datetime(2026, 4, 28)
    cursor.execute.assert_called_once_with(
        "DELETE FROM login_events WHERE ts >= %s AND ts < %s",
        (day_start, day_end),
    )
    cursor.executemany.assert_called_once()
    sql, rows = cursor.executemany.call_args.args
    assert "INSERT INTO login_events (email, ts, ip, event_json, source_topic)" in sql
    assert rows == [
        ("a@x.com", "2026-04-27T10:00:00", "1.1.1.1", events[0]["event_json"], "system_1"),
        ("b@x.com", "2026-04-27T10:05:00", "1.1.1.1", events[1]["event_json"], "system_2"),
    ]
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_insert_login_events_with_no_events_still_runs_delete(pg_env):
    conn, cursor = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        insert_login_events([], "2026-04-28")

    cursor.execute.assert_called_once()
    cursor.executemany.assert_not_called()
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_insert_login_events_rolls_back_and_reraises_on_error(pg_env):
    conn, cursor = _mock_conn()
    cursor.execute.side_effect = RuntimeError("db down")

    with patch.object(db, "get_connection", return_value=conn):
        with pytest.raises(RuntimeError, match="db down"):
            insert_login_events([_event("a@x.com", "2026-04-27T10:00:00", "1.1.1.1")], "2026-04-28")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_insert_login_events_closes_connection_in_finally(pg_env):
    conn, _ = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        insert_login_events([], "2026-04-28")
    conn.close.assert_called_once()


def test_insert_identity_link_history_deletes_window_then_runs_sql_file(pg_env):
    conn, cursor = _mock_conn()
    cursor.rowcount = 7
    with patch.object(db, "get_connection", return_value=conn):
        result = insert_identity_link_history("2026-04-28")

    day_start = datetime(2026, 4, 27)
    day_end = datetime(2026, 4, 28)

    assert cursor.execute.call_count == 2

    delete_sql, delete_args = cursor.execute.call_args_list[0].args
    assert "DELETE FROM identity_links_history" in delete_sql
    assert "COALESCE(system_1_ts, system_2_ts, system_3_ts)" in delete_sql
    assert delete_args == (day_start, day_end)

    insert_sql, insert_args = cursor.execute.call_args_list[1].args
    assert "INSERT INTO identity_links_history" in insert_sql
    assert "FILTER (WHERE source_topic = 'system_1')" in insert_sql
    assert insert_args == (day_start, day_end)

    assert result == 7
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_insert_identity_link_history_rolls_back_and_reraises_on_error(pg_env):
    conn, cursor = _mock_conn()
    cursor.execute.side_effect = RuntimeError("db down")

    with patch.object(db, "get_connection", return_value=conn):
        with pytest.raises(RuntimeError, match="db down"):
            insert_identity_link_history("2026-04-28")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_insert_identity_link_history_closes_connection_in_finally(pg_env):
    conn, _ = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        insert_identity_link_history("2026-04-28")
    conn.close.assert_called_once()


def test_insert_identity_link_runs_merge_sql_file(pg_env):
    conn, cursor = _mock_conn()
    cursor.rowcount = 4
    with patch.object(db, "get_connection", return_value=conn):
        result = insert_identity_link("2026-04-28")

    day_start = datetime(2026, 4, 27)
    day_end = datetime(2026, 4, 28)

    cursor.execute.assert_called_once()
    merge_sql, args = cursor.execute.call_args.args
    assert "MERGE INTO identity_links" in merge_sql
    assert "FROM identity_links_history" in merge_sql
    assert args == (day_start, day_end)

    assert result == 4
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_insert_identity_link_rolls_back_and_reraises_on_error(pg_env):
    conn, cursor = _mock_conn()
    cursor.execute.side_effect = RuntimeError("db down")

    with patch.object(db, "get_connection", return_value=conn):
        with pytest.raises(RuntimeError, match="db down"):
            insert_identity_link("2026-04-28")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_insert_identity_link_closes_connection_in_finally(pg_env):
    conn, _ = _mock_conn()
    with patch.object(db, "get_connection", return_value=conn):
        insert_identity_link("2026-04-28")
    conn.close.assert_called_once()
