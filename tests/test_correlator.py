from src.correlator import find_correlated_pairs


def _evt(email, ts, ip):
    return {"email": email, "ts": ts, "ip": ip, "event_json": "{}", "source_topic": "system_1"}


def test_pairs_two_emails_within_window():
    events = [
        _evt("a@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
        _evt("b@x.com", "2026-04-27T10:05:00", "1.1.1.1"),
    ]
    pairs = find_correlated_pairs(events)
    assert len(pairs) == 1
    email_a, email_b, ip, diff = pairs[0]
    assert {email_a, email_b} == {"a@x.com", "b@x.com"}
    assert ip == "1.1.1.1"
    assert diff == 300.0


def test_skips_events_outside_window():
    events = [
        _evt("a@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
        _evt("b@x.com", "2026-04-27T10:11:00", "1.1.1.1"),
    ]
    assert find_correlated_pairs(events) == []


def test_boundary_at_exactly_600_seconds_is_included():
    events = [
        _evt("a@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
        _evt("b@x.com", "2026-04-27T10:10:00", "1.1.1.1"),
    ]
    pairs = find_correlated_pairs(events)
    assert len(pairs) == 1
    assert pairs[0][3] == 600.0


def test_skips_pairs_with_same_email():
    events = [
        _evt("a@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
        _evt("a@x.com", "2026-04-27T10:01:00", "1.1.1.1"),
    ]
    assert find_correlated_pairs(events) == []


def test_does_not_pair_across_different_ips():
    events = [
        _evt("a@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
        _evt("b@x.com", "2026-04-27T10:01:00", "2.2.2.2"),
    ]
    assert find_correlated_pairs(events) == []


def test_three_emails_on_same_ip_produces_three_pairs():
    events = [
        _evt("a@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
        _evt("b@x.com", "2026-04-27T10:01:00", "1.1.1.1"),
        _evt("c@x.com", "2026-04-27T10:02:00", "1.1.1.1"),
    ]
    pairs = find_correlated_pairs(events)
    assert len(pairs) == 3
    pair_keys = {tuple(sorted([p[0], p[1]])) for p in pairs}
    assert pair_keys == {
        ("a@x.com", "b@x.com"),
        ("a@x.com", "c@x.com"),
        ("b@x.com", "c@x.com"),
    }


def test_empty_events_returns_empty():
    assert find_correlated_pairs([]) == []


def test_diff_is_absolute_value():
    events = [
        _evt("a@x.com", "2026-04-27T10:05:00", "1.1.1.1"),
        _evt("b@x.com", "2026-04-27T10:00:00", "1.1.1.1"),
    ]
    pairs = find_correlated_pairs(events)
    assert pairs[0][3] == 300.0
