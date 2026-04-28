import argparse
import logging

from dotenv import load_dotenv

from src.correlator import find_correlated_pairs
from src.db import (
    insert_identity_link,
    insert_identity_link_history,
    insert_login_events,
    upsert_identity_links,
)
from src.kafka_consumer import consume_events, make_consumer
from src.union_find import build_groups

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("identity_pipeline")


def run(date, backfill=False):
    consumer = make_consumer(date, backfill=backfill)
    try:
        logger.info("Step 1: consuming events from Kafka for %s (backfill=%s)", date, backfill)
        events = consume_events(consumer, date, backfill=backfill)
        logger.info("Step 1 done: %d events", len(events))

        logger.info("Step 2: inserting login events into DB")
        insert_login_events(events, date)
        logger.info("Step 2 done: %d events inserted", len(events))

        logger.info("Step 2b: writing identity_links_history")
        inserted = insert_identity_link_history(date)
        logger.info("Step 2b done: %d history rows", inserted)

        logger.info("Step 2c: merging into identity_links")
        affected = insert_identity_link(date)
        logger.info("Step 2c done: %d rows merged", affected)

        if not backfill:
            consumer.commit(asynchronous=False)
            logger.info("Kafka offsets committed")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Identity pipeline daily job")
    parser.add_argument("--date", required=True, help="DAG execution date (YYYY-MM-DD)")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill mode: per-run group id, in-process window filter, no offset commit",
    )
    args = parser.parse_args()
    run(args.date, backfill=args.backfill)


if __name__ == "__main__":
    main()