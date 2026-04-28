# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Status

`identity_pipeline_spec.md` is the source of truth for behavior. `main.py`, `src/`, `Dockerfile`, and `requirements.txt` implement that spec. Tests live in `tests/`.

## What This Job Does

A daily Python batch job, packaged as a Docker container and triggered by Airflow, that:

1. Consumes login events from Kafka topics `system_1`, `system_2`, `system_3` for the previous day's window (`day_start = --date - 1d`, `day_end = --date`).
2. Persists the parsed events into the PostgreSQL `login_events` table (idempotent: deletes the same `[day_start, day_end)` ts range first, then inserts).
3. Pivots `login_events` into `identity_links_history` — one row per IP that had activity across systems within a 10-minute window, with the latest `(email, ts)` per system collapsed into `system_{1,2,3}_email/_ts` columns. Idempotent via delete-then-insert on the day window. SQL lives in `sql/insert_into_identity_links_history.sql`.
4. Merges `identity_links_history` for the day into `identity_links` via PostgreSQL `MERGE`. Source is deduped (`SELECT DISTINCT` on the three email columns); match condition is OR across the three positional `system_N_email` columns; on match, existing emails are preserved (`COALESCE(t.col, s.col)`) and `last_updated = CURRENT_TIMESTAMP`; on no match, insert. SQL lives in `sql/insert_into_identity_links.sql`. Requires PostgreSQL 15+.

The four steps are pure-functional pipes — no shared state, all data flows as function arguments between modules. Each module is independently testable.

`identity_links` schema (current):

```sql
CREATE TABLE identity_links (
    id             SERIAL PRIMARY KEY,
    system_1_email VARCHAR NULL,
    system_2_email VARCHAR NULL,
    system_3_email VARCHAR NULL,
    last_updated   TIMESTAMP NOT NULL
);
```

The earlier `(emails text[], last_updated date)` schema and the `find_correlated_pairs` → `build_groups` → `upsert_identity_links` Union-Find path are dead code; `correlator.py`, `union_find.py`, and `upsert_identity_links` remain in-tree but are not invoked by `main.run`.

## Architecture: Non-Obvious Constraints

These are easy to miss but load-bearing — read the spec section before changing any of them:

- **Idempotency via delete-then-insert (login_events, identity_links_history).** `insert_login_events` runs `DELETE FROM login_events WHERE ts >= day_start AND ts < day_end`, then inserts. `insert_identity_link_history` does the same on `identity_links_history` filtered by `COALESCE(system_1_ts, system_2_ts, system_3_ts)`. Re-running the job for the same date must produce the same end state for these two tables. Don't switch to plain INSERT or `ON CONFLICT` without preserving this property.
- **`identity_links` is mutated in place by `MERGE`, not delete-then-insert.** `insert_identity_link` does NOT delete; it OR-matches on the three `system_N_email` columns and either updates (`COALESCE` to preserve existing emails, `last_updated = CURRENT_TIMESTAMP`) or inserts. Re-running for the same day will refresh `last_updated` on matched rows but won't duplicate them. `last_updated` is wall-clock now, not `yesterday` — operators tracking "which day's data" use the per-day rows in `identity_links_history`.
- **MERGE caveat: OR-ON can raise `MERGE command cannot affect row a second time`** when one source row matches multiple target rows or vice versa. If correlated rows pile up over time, expect to add a pre-aggregation step on the target side.
- **Kafka consumption has two strategies, selected by `--backfill`.**
  - *Incremental (default)*: stable `group.id = "identity_pipeline"`, `enable.auto.commit=False`. The consumer resumes from the last committed offset; `main.run` calls `consumer.commit(asynchronous=False)` AFTER all DB writes succeed. The in-process window filter is OFF — incremental runs only see events past the last commit, so there's nothing outside the window to drop. Crash/DB failure between consume and commit is safe: idempotent DB writes + uncommitted offsets mean Airflow retry replays cleanly.
  - *Backfill (`--backfill`)*: per-date `group.id = f"identity_pipeline_{date}"`, `auto.offset.reset=earliest`, no commit. Reads from the start of the retention window every time; the in-process window filter (`day_start <= event_ts < day_end`) scopes the result to the requested day. Use this to re-process a historical day without disturbing the production offset.
- **The consumer outlives `consume_events`.** `main.run` calls `make_consumer()` → `consume_events()` → DB writes → `commit()` → `close()` (the last in `finally`). Don't push `commit` inside `consume_events` — that would commit before the DB write, defeating the at-least-once guarantee.
- **Stop condition is empty polls, not EOF.** Consumer stops after **3 consecutive empty polls** (timeout=5.0s). There is no end-of-partition signal in this design.
- **Email ordering for pairs is deferred to the DB layer.** `correlator.py` does NOT enforce `email_a < email_b`; that normalization happens in `db.py`. Don't add it in two places.
- **`get_groups()` returns only groups with size > 1.** Singletons are filtered out — that's the contract, not a bug.
- **All DB functions own their connection.** Each function opens, uses, and closes its own connection in a `finally` block. Don't introduce a shared connection pool without revisiting this.
- **Parameterized queries only.** Use `%s` placeholders; never f-string user input into SQL.

## CLI

```bash
# Incremental (default): resumes from last committed Kafka offset, commits on success.
python main.py --date 2026-04-28   # processes events for 2026-04-27 00:00 → 2026-04-28 00:00

# Backfill: replays from earliest, in-process window filter, never commits.
python main.py --date 2026-04-15 --backfill
```

The `--date` argument is the **DAG execution date**; the actual data window is the day before.

## Build & Run

```bash
docker build -t identity-pipeline .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e POSTGRES_HOST=... -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=identity_db -e POSTGRES_USER=... -e POSTGRES_PASSWORD=... \
  identity-pipeline --date 2026-04-27
```

Required env vars: `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`. `db.get_connection()` should fail fast with a clear error if any are missing.

## Dependencies

Pinned in `requirements.txt` (per spec): `confluent-kafka==2.3.0`, `psycopg2-binary==2.9.9`. Base image: `python:3.11-slim`.

## Logging Expectations

`main.py` logs the start of each step and the **count** of results after each step (events, pairs, groups), then `"Done"` on success. Preserve this — it's the operator's only observability into a daily batch run.

## Testing Discipline

**Every function in this repo has a corresponding test, and changes must keep that property.** When you add, rename, or change the signature/behavior of a function, update or add a test in `tests/` in the same change — don't ship code without it.

- Layout: `tests/test_<module>.py` (one file per source module: `test_union_find.py`, `test_correlator.py`, `test_kafka_consumer.py`, `test_db.py`, `test_main.py`).
- Framework: **pytest** (added in `requirements-dev.txt`). External systems are mocked: `confluent_kafka.Consumer` is patched in consumer tests, `psycopg2` and `get_connection` are patched in db tests, every step function called by `main.run` is patched in `test_main.py`.
- Cover the non-obvious constraints listed above, not just the happy path: window boundaries (the `<= 600s` cutoff, the `< day_end` exclusive bound), the "stop after 3 empty polls" rule, the empty-poll counter resetting on a real message, the delete-then-insert order for `login_events` and `identity_links_history`, the `MERGE`-runs-the-SQL-file path for `identity_links`, `finally`-block close on both success and exception, env-var validation in `get_connection`.
- Install dev deps with `pip install -r requirements-dev.txt`. Run the full suite with `python -m pytest -q`. Run a single test with `python -m pytest tests/test_correlator.py::test_boundary_at_exactly_600_seconds_is_included`.
- Tests must pass before declaring a task complete.