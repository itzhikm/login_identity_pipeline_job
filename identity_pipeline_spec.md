# Identity Pipeline Job Spec for Claude Code

## Overview
A Python batch job that runs daily as a Docker container, triggered by Airflow.
It reads login events from Kafka, finds correlated identity pairs by IP and time window,
builds identity groups using Union-Find, and writes the results to PostgreSQL.

---

## Project Structure
```
identity-pipeline/
├── Dockerfile
├── requirements.txt
├── main.py
└── src/
    ├── kafka_consumer.py
    ├── correlator.py
    ├── union_find.py
    └── db.py
```

---

## Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=identity_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret
```

---

## main.py

Entry point for the job. Orchestrates the 4 steps in sequence with logging.

```
Steps:
  1. consume_from_kafka(date)       → events: list[dict]
  2. find_correlated_pairs(events)  → pairs:  list[tuple]
  3. build_groups(pairs)            → groups: list[list[str]]
  4. upsert_identity_links(groups, date)

CLI:
  python main.py --date 2026-04-27

Logging:
  - Log start of each step
  - Log count of results after each step (events, pairs, groups)
  - Log "Done" on success
```

---

## src/kafka_consumer.py

Reads login events from Kafka for a given date (previous day based on --date argument).

```
Function:
  consume_from_kafka(date: str) -> list[dict]

Behavior:
  - date argument is the DAG execution date (e.g. 2026-04-28)
  - day_start = date - 1 day  (2026-04-27 00:00:00)
  - day_end   = date          (2026-04-28 00:00:00)
  - Subscribe to topics: system_1, system_2, system_3
  - Consumer group.id = f'identity_pipeline_{date}'  (unique per run)
  - auto.offset.reset = earliest
  - enable.auto.commit = False
  - Poll with timeout=5.0 seconds
  - Stop after 3 consecutive empty polls
  - Filter events by event time (ts field): day_start <= event_ts < day_end
  - Late arrivals outside this window are silently skipped

Each returned event dict contains:
  {
    "email":        str,
    "ts":           str,   # ISO 8601 original event time
    "ip":           str,
    "event_json":   str,   # raw JSON string of original payload
    "source_topic": str    # which Kafka topic the event came from
  }

Error handling:
  - Raise KafkaException on message error
  - Always call consumer.close() in finally block
```

---

## src/correlator.py

Finds pairs of different identities that appeared on the same IP within 10 minutes.

```
Function:
  find_correlated_pairs(events: list[dict]) -> list[tuple]

Algorithm:
  1. Group events by IP address
  2. For each IP group, compare all pairs (i, j) where i < j
  3. Skip pairs where email_a == email_b
  4. Calculate time difference in seconds between ts_a and ts_b
  5. If diff <= 600 seconds → add to pairs

Each returned tuple contains:
  (email_a: str, email_b: str, ip: str, time_diff_seconds: float)

Notes:
  - email_a < email_b ordering is NOT enforced here (handled in DB layer)
  - Time comparison uses datetime.fromisoformat()
```

---

## src/union_find.py

Builds identity groups from correlated pairs using Union-Find with path compression.

```
Class: UnionFind
  __init__()         → initializes empty parent dict
  find(x)            → returns root of x, with path compression
  union(x, y)        → merges the groups of x and y
  get_groups()       → returns list of groups, each group is a list of emails
                       only groups with more than 1 email are returned

Function:
  build_groups(pairs: list[tuple]) -> list[list[str]]
    - Creates UnionFind instance
    - Calls union(email_a, email_b) for each pair
    - Returns uf.get_groups()
```

---

## src/db.py

Handles all PostgreSQL interactions for the pipeline job.

```
Function:
  get_connection()
    - Connects using environment variables:
      POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    - Raises clear error if any variable is missing

Function:
  upsert_identity_links(groups: list[list[str]], date: str)
    - yesterday = date - 1 day
    - Opens a single connection for the entire operation
    - Step 1 – idempotency: DELETE FROM identity_links WHERE last_updated = yesterday
    - Step 2 – insert: for each group INSERT INTO identity_links (emails, last_updated)
    - Commits after all inserts
    - Always closes connection in finally block

Notes:
  - Use %s placeholders, never f-strings with user input
  - Log DB errors before re-raising
```

---

## Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENTRYPOINT ["python", "main.py"]
```

---

## requirements.txt
```
confluent-kafka==2.3.0
psycopg2-binary==2.9.9
```

---

## Local Debug Run

```bash
docker build -t identity-pipeline .

docker run --rm identity-pipeline \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=identity_db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=secret \
  --date 2026-04-27
```

---

## Implementation Notes
- Each module is independently testable
- No shared state between modules — all data passed as function arguments
- All DB functions open and close their own connection
- Use executemany for bulk inserts where possible
- Log count of results after each step for observability
