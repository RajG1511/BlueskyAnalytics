import asyncio
import json
import logging
import os
import random
from datetime import datetime

import aiofiles
import websockets
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# --- Jetstream endpoints (failover order) ---
JETSTREAM_ENDPOINTS = [
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-west.bsky.network/subscribe",
]

# AT Protocol collection strings → Kafka topic names
COLLECTION_TO_TOPIC = {
    "app.bsky.feed.post":    "bluesky.feed.post",
    "app.bsky.feed.like":    "bluesky.feed.like",
    "app.bsky.feed.repost":  "bluesky.feed.repost",
    "app.bsky.graph.follow": "bluesky.graph.follow",
    "app.bsky.graph.block":  "bluesky.graph.block",
}

DEAD_LETTER_TOPIC = "bluesky.dead-letter"
CURSOR_FILE       = "cursor.json"
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


async def load_cursor() -> int | None:
    """Read the last saved cursor from disk. Returns None if no cursor exists."""
    try:
        async with aiofiles.open(CURSOR_FILE, "r") as f:
            data = json.loads(await f.read())
            return data.get("cursor")
    except FileNotFoundError:
        return None


async def save_cursor(cursor_us: int) -> None:
    """Persist the latest event timestamp to disk for resume-on-restart."""
    async with aiofiles.open(CURSOR_FILE, "w") as f:
        await f.write(json.dumps({
            "cursor":   cursor_us,
            "saved_at": datetime.utcnow().isoformat()
        }))


def parse_event(raw_json: str) -> dict | None:
    """
    Parse and minimally validate a Jetstream JSON event.
    Returns None if the event is malformed or not a commit event.
    """
    try:
        event = json.loads(raw_json)
    except json.JSONDecodeError as e:
        log.warning(f"JSON decode error: {e}")
        return None

    if event.get("kind") != "commit":
        return None  # skip identity/account events

    commit = event.get("commit", {})
    if not commit.get("collection") or not commit.get("operation"):
        return None

    return event


def build_record(event: dict, topic: str) -> dict | None:
    """
    Flatten a raw Jetstream event into a clean dict for the given topic.
    This is what gets JSON-serialized and sent to Kafka.
    """
    commit  = event["commit"]
    record  = commit.get("record", {})

    base = {
        "did":       event["did"],
        "time_us":   event["time_us"],
        "operation": commit["operation"],
        "rkey":      commit.get("rkey", ""),
        "raw_json":  json.dumps(event),   # full original payload for Bronze layer
    }

    if topic == "bluesky.feed.post":
        reply = record.get("reply", {})
        return {
            **base,
            "cid":          commit.get("cid"),
            "text":         record.get("text"),
            "created_at":   record.get("createdAt"),
            "langs":        record.get("langs", []),
            "reply_parent": reply.get("parent", {}).get("uri") if reply else None,
            "reply_root":   reply.get("root",   {}).get("uri") if reply else None,
            "has_embed":    "embed" in record,
        }

    if topic in ("bluesky.feed.like", "bluesky.feed.repost"):
        subject = record.get("subject", {})
        return {
            **base,
            "subject_uri": subject.get("uri"),
            "subject_cid": subject.get("cid"),
        }

    if topic in ("bluesky.graph.follow", "bluesky.graph.block"):
        return {
            **base,
            "subject_did": record.get("subject"),
        }

    return None


def make_producer() -> Producer:
    """Create a confluent-kafka Producer connected to the cluster."""
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "acks":               "all",
        "retries":            5,
        "retry.backoff.ms":   500,
        "enable.idempotence": True,
        "compression.type":   "lz4",
        "linger.ms":          5,
        "batch.num.messages": 1000,
    })


def on_delivery(err, msg):
    """Callback fired by librdkafka when a message is ack'd or fails."""
    if err:
        log.error(f"Delivery failed for topic {msg.topic()}: {err}")
    else:
        log.debug(f"Delivered → {msg.topic()} partition {msg.partition()} offset {msg.offset()}")


async def consume(producer: Producer) -> None:
    """
    Main loop: connect to Jetstream, receive events, parse and publish to Kafka.
    Handles reconnection with exponential backoff and cursor-based resume.
    """
    endpoint_idx     = 0
    backoff          = 1.0
    max_backoff      = 60.0
    events_count     = 0
    last_cursor_save = 0

    while True:
        cursor   = await load_cursor()
        base_url = JETSTREAM_ENDPOINTS[endpoint_idx % len(JETSTREAM_ENDPOINTS)]

        collections = "&".join(
            f"wantedCollections={c}" for c in COLLECTION_TO_TOPIC.keys()
        )
        url = f"{base_url}?{collections}"

        if cursor:
            resume_cursor = cursor - 5_000_000  # rewind 5s to avoid gaps
            url += f"&cursor={resume_cursor}"
            log.info(f"Resuming from cursor {resume_cursor} via {base_url}")
        else:
            log.info(f"Starting fresh connection to {base_url}")

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                log.info("WebSocket connected.")
                backoff = 1.0  # reset on successful connect

                async for message in ws:
                    event = parse_event(message)

                    if event is None:
                        # malformed or non-commit — route to dead-letter raw
                        producer.produce(
                            topic=DEAD_LETTER_TOPIC,
                            value=message.encode("utf-8"),
                            on_delivery=on_delivery,
                        )
                        producer.poll(0)
                        continue

                    collection = event["commit"]["collection"]
                    topic = COLLECTION_TO_TOPIC.get(collection)

                    if topic is None:
                        continue  # collection we don't care about

                    record = build_record(event, topic)
                    if record is None:
                        continue

                    # Serialize to JSON bytes and produce to Kafka
                    producer.produce(
                        topic=topic,
                        key=event["did"].encode("utf-8"),
                        value=json.dumps(record).encode("utf-8"),
                        on_delivery=on_delivery,
                    )
                    producer.poll(0)

                    events_count += 1

                    # Save cursor every 1000 events
                    if events_count - last_cursor_save >= 1000:
                        await save_cursor(event["time_us"])
                        last_cursor_save = events_count
                        log.info(f"Cursor saved. Total events processed: {events_count}")

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e}. Reconnecting...")
        except Exception as e:
            log.error(f"Unexpected error: {e}. Reconnecting...")

        jitter = random.uniform(0, backoff * 0.3)
        wait   = min(backoff + jitter, max_backoff)
        log.info(f"Waiting {wait:.1f}s before reconnect...")
        await asyncio.sleep(wait)

        backoff = min(backoff * 2, max_backoff)
        endpoint_idx += 1


async def main():
    log.info("Starting Bluesky consumer service...")
    producer = make_producer()

    try:
        await consume(producer)
    finally:
        log.info("Flushing producer...")
        producer.flush(timeout=30)
        log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())