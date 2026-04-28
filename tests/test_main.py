from unittest.mock import MagicMock, patch

import pytest

import main


def _events():
    return [{"email": "a@x.com", "ts": "2026-04-27T10:00:00", "ip": "1.1.1.1",
             "event_json": "{}", "source_topic": "system_1"}]


def test_run_incremental_orchestrates_steps_and_commits_offsets():
    fake_consumer = MagicMock()
    events = _events()

    with patch.object(main, "make_consumer", return_value=fake_consumer) as m_make, \
         patch.object(main, "consume_events", return_value=events) as m_consume, \
         patch.object(main, "insert_login_events") as m_insert, \
         patch.object(main, "insert_identity_link_history", return_value=3) as m_history, \
         patch.object(main, "insert_identity_link", return_value=2) as m_link:
        main.run("2026-04-28")

    m_make.assert_called_once_with("2026-04-28", backfill=False)
    m_consume.assert_called_once_with(fake_consumer, "2026-04-28", backfill=False)
    m_insert.assert_called_once_with(events, "2026-04-28")
    m_history.assert_called_once_with("2026-04-28")
    m_link.assert_called_once_with("2026-04-28")
    fake_consumer.commit.assert_called_once_with(asynchronous=False)
    fake_consumer.close.assert_called_once()


def test_run_backfill_does_not_commit_offsets():
    fake_consumer = MagicMock()

    with patch.object(main, "make_consumer", return_value=fake_consumer) as m_make, \
         patch.object(main, "consume_events", return_value=[]) as m_consume, \
         patch.object(main, "insert_login_events"), \
         patch.object(main, "insert_identity_link_history", return_value=0), \
         patch.object(main, "insert_identity_link", return_value=0):
        main.run("2026-04-28", backfill=True)

    m_make.assert_called_once_with("2026-04-28", backfill=True)
    m_consume.assert_called_once_with(fake_consumer, "2026-04-28", backfill=True)
    fake_consumer.commit.assert_not_called()
    fake_consumer.close.assert_called_once()


def test_run_does_not_commit_when_db_write_fails():
    fake_consumer = MagicMock()

    with patch.object(main, "make_consumer", return_value=fake_consumer), \
         patch.object(main, "consume_events", return_value=_events()), \
         patch.object(main, "insert_login_events", side_effect=RuntimeError("db down")), \
         patch.object(main, "insert_identity_link_history"), \
         patch.object(main, "insert_identity_link"):
        with pytest.raises(RuntimeError, match="db down"):
            main.run("2026-04-28")

    fake_consumer.commit.assert_not_called()
    fake_consumer.close.assert_called_once()


def test_run_closes_consumer_when_consume_raises():
    fake_consumer = MagicMock()

    with patch.object(main, "make_consumer", return_value=fake_consumer), \
         patch.object(main, "consume_events", side_effect=RuntimeError("kafka boom")):
        with pytest.raises(RuntimeError, match="kafka boom"):
            main.run("2026-04-28")

    fake_consumer.commit.assert_not_called()
    fake_consumer.close.assert_called_once()


def test_main_parses_date_arg_and_runs_incremental_by_default():
    with patch.object(main, "run") as m_run, \
         patch("sys.argv", ["main.py", "--date", "2026-04-28"]):
        main.main()
    m_run.assert_called_once_with("2026-04-28", backfill=False)


def test_main_parses_backfill_flag():
    with patch.object(main, "run") as m_run, \
         patch("sys.argv", ["main.py", "--date", "2026-04-28", "--backfill"]):
        main.main()
    m_run.assert_called_once_with("2026-04-28", backfill=True)