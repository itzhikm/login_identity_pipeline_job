import json
from unittest.mock import MagicMock, patch

import pytest

from src import kafka_consumer
from src.kafka_consumer import consume_events, make_consumer


class FakeMsg:
    def __init__(self, topic, value, error=None):
        self._topic = topic
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return self._error


def _payload(email="a@x.com", ts="2026-04-27T10:00:00", ip="1.1.1.1"):
    return json.dumps({"email": email, "ts": ts, "ip": ip}).encode("utf-8")


@pytest.fixture(autouse=True)
def kafka_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def test_consume_events_returns_polled_messages_in_incremental_mode():
    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [
        FakeMsg("system_1", _payload(email="a@x.com", ts="2026-04-27T10:00:00")),
        FakeMsg("system_2", _payload(email="b@x.com", ts="2026-04-27T11:00:00")),
        None, None, None,
    ]

    events = consume_events(fake_consumer, "2026-04-28")

    assert len(events) == 2
    assert events[0]["email"] == "a@x.com"
    assert events[0]["source_topic"] == "system_1"
    assert events[1]["source_topic"] == "system_2"
    assert "event_json" in events[0]


def test_incremental_mode_does_not_filter_by_window():
    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [
        FakeMsg("system_1", _payload(ts="2026-04-26T23:59:59")),
        FakeMsg("system_1", _payload(ts="2026-04-28T00:00:00")),
        FakeMsg("system_1", _payload(ts="2026-04-27T12:00:00")),
        None, None, None,
    ]

    events = consume_events(fake_consumer, "2026-04-28", backfill=False)

    assert len(events) == 3


def test_backfill_mode_filters_events_outside_window():
    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [
        FakeMsg("system_1", _payload(ts="2026-04-26T23:59:59")),  # before
        FakeMsg("system_1", _payload(ts="2026-04-28T00:00:00")),  # at end (excluded)
        FakeMsg("system_1", _payload(ts="2026-04-27T12:00:00")),  # in window
        None, None, None,
    ]

    events = consume_events(fake_consumer, "2026-04-28", backfill=True)

    assert len(events) == 1
    assert events[0]["ts"] == "2026-04-27T12:00:00"


def test_stops_after_three_consecutive_empty_polls():
    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [None, None, None]

    events = consume_events(fake_consumer, "2026-04-28")

    assert events == []
    assert fake_consumer.poll.call_count == 3


def test_empty_poll_counter_resets_on_message():
    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [
        None, None,
        FakeMsg("system_1", _payload(ts="2026-04-27T10:00:00")),
        None, None, None,
    ]

    events = consume_events(fake_consumer, "2026-04-28")

    assert len(events) == 1
    assert fake_consumer.poll.call_count == 6


def test_kafka_message_error_raises():
    from confluent_kafka import KafkaException

    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [FakeMsg("system_1", b"", error="broken")]

    with pytest.raises(KafkaException):
        consume_events(fake_consumer, "2026-04-28")


def test_make_consumer_incremental_uses_stable_group_id():
    fake_consumer = MagicMock()
    with patch.object(kafka_consumer, "Consumer", return_value=fake_consumer) as ctor:
        make_consumer("2026-04-28", backfill=False)

    config = ctor.call_args.args[0]
    assert config["group.id"] == "identity_pipeline"
    assert config["auto.offset.reset"] == "earliest"
    assert config["enable.auto.commit"] is False
    fake_consumer.subscribe.assert_called_once_with(["system_1", "system_2", "system_3"])


def test_make_consumer_backfill_uses_per_date_group_id():
    fake_consumer = MagicMock()
    with patch.object(kafka_consumer, "Consumer", return_value=fake_consumer) as ctor:
        make_consumer("2026-04-28", backfill=True)

    config = ctor.call_args.args[0]
    assert config["group.id"] == "identity_pipeline_2026-04-28"
    assert config["enable.auto.commit"] is False
    fake_consumer.subscribe.assert_called_once_with(["system_1", "system_2", "system_3"])


def test_make_consumer_defaults_to_incremental():
    fake_consumer = MagicMock()
    with patch.object(kafka_consumer, "Consumer", return_value=fake_consumer) as ctor:
        make_consumer("2026-04-28")

    config = ctor.call_args.args[0]
    assert config["group.id"] == "identity_pipeline"