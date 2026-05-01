"""Zonestream → hourly NDJSON.zst partitions.

Designed for a GitHub Actions runner with a hard wall-clock limit:
- ZS_RUN_SECONDS  : how long to consume before exiting cleanly (default 5h45m)
- ZS_TOPICS       : comma-separated Kafka topic list
- ZS_GROUP_ID     : Kafka consumer group; broker is the offset authority
- ZS_OFFSET_RESET : 'earliest' (default) or 'latest', honored only when the
                    group has no committed offset
- ZS_FLUSH_SECONDS: fsync + commit cadence in seconds (default 60)

Writes to data/<topic>/YYYY/MM/DD/HH.jsonl.zst, appending raw broker payloads
(one JSON document per line, no reformatting). Offsets are committed to the
broker only after the local zstd writer has been flushed, so at-least-once
delivery holds.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

import zstandard as zstd
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.environ.get(
    "ZS_BOOTSTRAP", "kafka.zonestream.openintel.nl:9092"
)
DATA_ROOT = Path("data")
STATE_PATH = Path("state/offsets.json")


def consumer_config() -> dict:
    # Zonestream Kafka is anonymous and unencrypted (PLAINTEXT on :9092),
    # see https://zonestream.openintel.nl/. No SASL or TLS client cert.
    return {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": os.environ["ZS_GROUP_ID"],
        "enable.auto.commit": False,
        "auto.offset.reset": os.environ.get("ZS_OFFSET_RESET", "earliest"),
        "security.protocol": "PLAINTEXT",
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 600000,
        "client.id": f"gha-{os.environ.get('GITHUB_RUN_ID', 'local')}",
    }


class HourlyWriters:
    """One zstd stream writer per (topic, UTC hour). Closed at flush time."""

    def __init__(self, level: int = 10):
        self._level = level
        self._open: dict[tuple[str, str], tuple[object, object]] = {}

    def write(self, topic: str, ts_ms: int, payload: bytes) -> Path:
        hour = time.strftime("%Y/%m/%d/%H", time.gmtime(ts_ms / 1000.0))
        key = (topic, hour)
        if key not in self._open:
            path = DATA_ROOT / topic / f"{hour}.jsonl.zst"
            path.parent.mkdir(parents=True, exist_ok=True)
            raw = open(path, "ab")
            cctx = zstd.ZstdCompressor(level=self._level)
            writer = cctx.stream_writer(raw, closefd=True)
            self._open[key] = (raw, writer)
        _, writer = self._open[key]
        writer.write(payload)
        if not payload.endswith(b"\n"):
            writer.write(b"\n")
        return DATA_ROOT / topic / f"{hour}.jsonl.zst"

    def flush_all(self) -> list[Path]:
        flushed: list[Path] = []
        for (topic, hour), (raw, writer) in list(self._open.items()):
            writer.flush(zstd.FLUSH_FRAME)
            raw.flush()
            os.fsync(raw.fileno())
            flushed.append(DATA_ROOT / topic / f"{hour}.jsonl.zst")
        return flushed

    def close_all(self) -> None:
        for (raw, writer) in self._open.values():
            writer.close()
            raw.close()
        self._open.clear()


def snapshot_offsets(consumer: Consumer) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    snap: dict[str, dict[str, int]] = {}
    assignments = consumer.assignment()
    if not assignments:
        return
    committed = consumer.committed(assignments, timeout=10)
    for tp in committed:
        snap.setdefault(tp.topic, {})[str(tp.partition)] = tp.offset
    snap["_meta"] = {
        "captured_at": int(time.time()),
        "group_id": os.environ["ZS_GROUP_ID"],
        "note": "Best-effort snapshot. Broker offsets are authoritative.",
    }
    STATE_PATH.write_text(json.dumps(snap, sort_keys=True, indent=2))


def main() -> int:
    run_seconds = int(os.environ.get("ZS_RUN_SECONDS", str(5 * 3600 + 45 * 60)))
    flush_seconds = int(os.environ.get("ZS_FLUSH_SECONDS", "60"))
    topics = [t.strip() for t in os.environ["ZS_TOPICS"].split(",") if t.strip()]
    if not topics:
        print("no topics configured", file=sys.stderr)
        return 2

    deadline = time.monotonic() + run_seconds

    consumer = Consumer(consumer_config())
    consumer.subscribe(topics)

    writers = HourlyWriters()
    stop = {"flag": False}

    def request_stop(*_):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    msgs_seen = 0
    msgs_flushed = 0
    last_flush = time.monotonic()

    try:
        while not stop["flag"] and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_flush >= flush_seconds:
                    writers.flush_all()
                    consumer.commit(asynchronous=False)
                    msgs_flushed = msgs_seen
                    last_flush = time.monotonic()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"kafka error: {msg.error()}", file=sys.stderr)
                continue

            ts_type, ts_ms = msg.timestamp()
            if ts_ms <= 0:
                ts_ms = int(time.time() * 1000)
            writers.write(msg.topic(), ts_ms, msg.value())
            msgs_seen += 1

            if time.monotonic() - last_flush >= flush_seconds:
                writers.flush_all()
                consumer.commit(asynchronous=False)
                msgs_flushed = msgs_seen
                last_flush = time.monotonic()
    finally:
        try:
            writers.flush_all()
            consumer.commit(asynchronous=False)
            msgs_flushed = msgs_seen
            snapshot_offsets(consumer)
        finally:
            writers.close_all()
            consumer.close()

    print(
        f"done: msgs_seen={msgs_seen} msgs_committed={msgs_flushed} "
        f"topics={','.join(topics)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
