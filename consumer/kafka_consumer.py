"""Zonestream → hourly partitions, two on-disk formats.

JSON topics land in ``data/<topic>/YYYY/MM/DD/HH.jsonl.zst`` (one JSON document
per line, raw broker payload, no reformatting).

Binary topics (Avro-on-the-wire) land in ``data/<topic>/YYYY/MM/DD/HH.kfb.zst``
using a length-prefixed framing — see KAFKA_FRAMING.md. We preserve the wire
bytes (Confluent magic + 4-byte schema id + Avro payload) plus the broker
timestamp so the records can be decoded later when the schema becomes known.

Env:
- ZS_RUN_SECONDS   wall-clock budget (default 5h45m)
- ZS_TOPICS        comma-separated topic list
- ZS_GROUP_ID      Kafka consumer group; broker is the offset authority
- ZS_OFFSET_RESET  earliest|latest, applied only when the group has no offsets
- ZS_FLUSH_SECONDS fsync + commit cadence (default 60)
- ZS_BOOTSTRAP     override broker (default kafka.zonestream.openintel.nl:9092)
"""

from __future__ import annotations

import json
import os
import signal
import struct
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
# Each run writes its own per-hour file so two runs can never collide on the
# same path (zstd files are not git-mergeable). Rollover still bundles all
# fragments for a calendar day into one release asset.
RUN_TAG = os.environ.get("GITHUB_RUN_ID", "local")

# Per-topic on-disk format. Anything not listed here defaults to "binary"
# (lossless framed bytes), which is the safe choice for an unknown topic.
TOPIC_FORMAT: dict[str, str] = {
    "newly_registered_domain": "json",
    "newly_registered_fqdn": "json",
    "confirmed_newly_registered_domain": "json",
    "certstream": "json",
    "certstream_domains": "json",
    "newly_issued_certificates_measurements": "binary",
    "newly_registered_domains_measurements": "binary",
    "newly_registered_fqdn_measurements": "binary",
}

# .kfb.zst frame: u64-BE timestamp_ms || u32-BE payload_len || payload bytes
_KFB_HDR = struct.Struct(">QI")


def consumer_config() -> dict:
    # Zonestream Kafka is anonymous and unencrypted (PLAINTEXT on :9092),
    # see https://openintel.nl/data/zonestream/. No SASL or TLS client cert.
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
    """One zstd stream writer per (topic, UTC hour)."""

    def __init__(self, level: int = 10):
        self._level = level
        self._open: dict[tuple[str, str], tuple[object, object, str]] = {}

    def _path(self, topic: str, hour: str) -> Path:
        fmt = TOPIC_FORMAT.get(topic, "binary")
        suffix = "jsonl.zst" if fmt == "json" else "kfb.zst"
        return DATA_ROOT / topic / f"{hour}-r{RUN_TAG}.{suffix}"

    def _open_writer(self, topic: str, hour: str):
        fmt = TOPIC_FORMAT.get(topic, "binary")
        path = self._path(topic, hour)
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = open(path, "ab")
        cctx = zstd.ZstdCompressor(level=self._level)
        writer = cctx.stream_writer(raw, closefd=True)
        return raw, writer, fmt

    def write(self, topic: str, ts_ms: int, payload: bytes) -> Path:
        hour = time.strftime("%Y/%m/%d/%H", time.gmtime(ts_ms / 1000.0))
        key = (topic, hour)
        if key not in self._open:
            self._open[key] = self._open_writer(topic, hour)
        _, writer, fmt = self._open[key]
        if fmt == "json":
            writer.write(payload)
            if not payload.endswith(b"\n"):
                writer.write(b"\n")
        else:
            writer.write(_KFB_HDR.pack(ts_ms, len(payload)))
            writer.write(payload)
        return self._path(topic, hour)

    def flush_all(self) -> list[Path]:
        flushed: list[Path] = []
        for (topic, hour), (raw, writer, _fmt) in list(self._open.items()):
            writer.flush(zstd.FLUSH_FRAME)
            raw.flush()
            os.fsync(raw.fileno())
            flushed.append(self._path(topic, hour))
        return flushed

    def close_all(self) -> None:
        for (raw, writer, _fmt) in self._open.values():
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

    print(f"topics: {topics}")
    print("formats: " + ", ".join(
        f"{t}={TOPIC_FORMAT.get(t, 'binary')}" for t in topics
    ))

    deadline = time.monotonic() + run_seconds

    consumer = Consumer(consumer_config())
    consumer.subscribe(topics)

    writers = HourlyWriters()
    stop = {"flag": False}

    def request_stop(*_):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    per_topic: dict[str, int] = {t: 0 for t in topics}
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
            per_topic[msg.topic()] = per_topic.get(msg.topic(), 0) + 1
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

    print(f"done: msgs_seen={msgs_seen} msgs_committed={msgs_flushed}")
    for t in topics:
        print(f"  {t}: {per_topic.get(t, 0)} msgs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
