"""Zonestream → hourly partitions, finalized into per-day flat files at
rollover time.

During a run, JSON topics are written to
``data/<topic>/_partial/YYYY-MM-DD/HH-r<runid>.jsonl.zst`` and binary topics
to ``…/HH-r<runid>.kfb.zst``. The daily ``rollover.yml`` workflow merges all
fragments for a calendar day into the canonical flat artifact
``data/<topic>/YYYY-MM-DD.jsonl.gz`` (matching the layout convention of
github.com/wangmm001/top-domains-aggregate) and uploads the same file as a
GitHub Release asset, then ``git rm``s the per-day _partial directory.

Broker offset durability
------------------------
This consumer **never advances broker-committed offsets**. It only writes
data to disk and snapshots the read positions it reached into
``state/offsets.json``. The workflow then commits the data to git, pushes
it, and only on push success calls ``consumer/finalize_offsets.py`` which
reads ``state/offsets.json`` and applies those offsets to the broker. If the
push fails, the broker offsets stay where they were and the next run
re-reads the same window — at-least-once with possible duplicates that
``pack_day.py`` deduplicates at rollover time. Without this discipline a
failed push silently loses data.

Topics with format "binary" (raw Avro-on-the-wire) use length-prefixed framing
— see KAFKA_FRAMING.md. We preserve wire bytes (Confluent magic + 4-byte
schema id + Avro payload) plus the broker timestamp so records can be replayed
through any future schema-aware decoder.

Topics with format "avro_extract" decode the Confluent-wire Avro payload
with a bundled schema (consumer/schemas/avro_id_1.json) and write a compact
JSON extract — see consumer/avro_extract.py. Used for the three DarkDNS
`*_measurements` topics which all share schema id=1.

Env:
- ZS_RUN_SECONDS    hard wall-clock budget (default 600s = 10 min)
- ZS_HWM_GRACE_SEC  after consumer position == broker high-water-mark for
                    every assigned partition, keep polling this many extra
                    seconds (default 5) before exiting. 0 disables → never
                    early-exits. Replaces a naive "no message for N seconds"
                    timer because live traffic on these topics is too dense
                    (~2 msg/s) to ever idle.
- ZS_TOPICS         comma-separated topic list
- ZS_GROUP_ID       Kafka consumer group; broker is the offset authority
- ZS_OFFSET_RESET   earliest|latest, applied only when the group has no offsets
- ZS_FLUSH_SECONDS  fsync cadence (default 60)
- ZS_BOOTSTRAP      override broker (default kafka.zonestream.openintel.nl:9092)
- ZS_MAX_MSGS_PER_TOPIC  optional hard cap on extracted/forwarded messages
                    per topic per run (default 0 = unlimited). Useful for the
                    high-volume `*_measurements` topics so a daily run produces
                    a bounded sample that fits in git/release.
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
from confluent_kafka import Consumer, KafkaError, TopicPartition

import avro_extract

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
    # The three DarkDNS `*_measurements` topics: Confluent-wire Avro on the
    # broker (schema id=1, `MeasurementResult` with 116-field `DnsRow`).
    # We decode + extract a compact subset (~6% of raw avro after gzip) —
    # see consumer/avro_extract.py for the extraction shape, and
    # KAFKA_FRAMING.md for how to instead opt into lossless `.kfb.zst`.
    "newly_issued_certificates_measurements": "avro_extract",
    "newly_registered_domains_measurements": "avro_extract",
    "newly_registered_fqdn_measurements": "avro_extract",
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
        # `hour` is "YYYY/MM/DD/HH" from time.gmtime; convert to the new
        # _partial/<YYYY-MM-DD>/<HH>-r<run>.<ext> layout.
        parts = hour.split("/")
        if len(parts) != 4:
            raise ValueError(f"unexpected hour format: {hour!r}")
        day = "-".join(parts[:3])
        hh = parts[3]
        fmt = TOPIC_FORMAT.get(topic, "binary")
        # avro_extract produces JSON lines, so it shares the .jsonl.zst
        # extension that pack_day.py already handles via pack_json().
        suffix = "kfb.zst" if fmt == "binary" else "jsonl.zst"
        return DATA_ROOT / topic / "_partial" / day / f"{hh}-r{RUN_TAG}.{suffix}"

    def _open_writer(self, topic: str, hour: str):
        fmt = TOPIC_FORMAT.get(topic, "binary")
        path = self._path(topic, hour)
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = open(path, "ab")
        cctx = zstd.ZstdCompressor(level=self._level)
        writer = cctx.stream_writer(raw, closefd=True)
        return raw, writer, fmt

    def write(self, topic: str, ts_ms: int, payload: bytes,
              offset: int | None = None) -> Path | None:
        """Write one message in its topic's format. Returns the destination
        path on success, or None if the message was skipped (e.g. Avro extract
        couldn't recognize the wire schema)."""
        hour = time.strftime("%Y/%m/%d/%H", time.gmtime(ts_ms / 1000.0))
        key = (topic, hour)
        if key not in self._open:
            self._open[key] = self._open_writer(topic, hour)
        _, writer, fmt = self._open[key]
        if fmt == "json":
            writer.write(payload)
            if not payload.endswith(b"\n"):
                writer.write(b"\n")
        elif fmt == "avro_extract":
            line = avro_extract.to_line(payload, ts_ms=ts_ms, offset=offset)
            if line is None:
                # Unknown wire schema — skip rather than write a broken line.
                # Caller bumps a counter.
                return None
            writer.write(line)
        else:  # "binary" — raw .kfb.zst framing
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
    """Snapshot the *read positions* this run reached.

    These are the offsets we WANT to commit to the broker, but only after
    the workflow has successfully pushed our data to git. ``finalize_offsets.py``
    reads this file and performs the actual broker commit.
    """
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    snap: dict[str, dict[str, int]] = {}
    assignments = consumer.assignment()
    if not assignments:
        return
    positions = consumer.position(assignments)
    for tp in positions:
        if tp.offset < 0:
            continue
        snap.setdefault(tp.topic, {})[str(tp.partition)] = int(tp.offset)
    snap["_meta"] = {
        "captured_at": int(time.time()),
        "group_id": os.environ["ZS_GROUP_ID"],
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "note": (
            "Read positions reached by this run. Apply with finalize_offsets.py "
            "ONLY after data has been durably pushed to git."
        ),
    }
    STATE_PATH.write_text(json.dumps(snap, sort_keys=True, indent=2))


def is_caught_up(consumer: Consumer) -> bool:
    """Has the consumer reached the broker's high-water mark on every partition?"""
    assignments = consumer.assignment()
    if not assignments:
        return False
    try:
        positions = {(tp.topic, tp.partition): tp.offset
                     for tp in consumer.position(assignments)}
        for tp in assignments:
            pos = positions.get((tp.topic, tp.partition), -1)
            if pos < 0:
                return False
            _, hwm = consumer.get_watermark_offsets(tp, timeout=5, cached=True)
            if pos < hwm:
                return False
        return True
    except Exception as exc:
        print(f"hwm check failed: {exc}", file=sys.stderr)
        return False


def main() -> int:
    run_seconds = int(os.environ.get("ZS_RUN_SECONDS", "600"))
    hwm_grace = int(os.environ.get("ZS_HWM_GRACE_SEC", "5"))
    flush_seconds = int(os.environ.get("ZS_FLUSH_SECONDS", "60"))
    max_per_topic = int(os.environ.get("ZS_MAX_MSGS_PER_TOPIC", "0"))
    hwm_check_seconds = 5
    topics = [t.strip() for t in os.environ["ZS_TOPICS"].split(",") if t.strip()]
    if not topics:
        print("no topics configured", file=sys.stderr)
        return 2

    print(f"topics: {topics}")
    print("formats: " + ", ".join(
        f"{t}={TOPIC_FORMAT.get(t, 'binary')}" for t in topics
    ))
    if max_per_topic > 0:
        print(f"per-topic cap: {max_per_topic} msgs")

    start = time.monotonic()
    deadline = start + run_seconds

    consumer = Consumer(consumer_config())
    consumer.subscribe(topics)

    writers = HourlyWriters()
    stop = {"flag": False, "reason": ""}

    def request_stop(*_):
        stop["flag"] = True
        stop["reason"] = "signal"

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    per_topic: dict[str, int] = {t: 0 for t in topics}
    per_topic_skipped: dict[str, int] = {t: 0 for t in topics}
    paused_topics: set[str] = set()
    msgs_seen = 0
    msgs_flushed = 0
    last_flush = time.monotonic()
    last_hwm_check = 0.0
    caught_up_since: float | None = None

    try:
        while not stop["flag"] and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                if now - last_flush >= flush_seconds:
                    writers.flush_all()
                    msgs_flushed = msgs_seen
                    last_flush = now
            else:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"kafka error: {msg.error()}", file=sys.stderr)
                    continue
                t = msg.topic()
                ts_type, ts_ms = msg.timestamp()
                if ts_ms <= 0:
                    ts_ms = int(time.time() * 1000)
                dest = writers.write(t, ts_ms, msg.value(), offset=msg.offset())
                if dest is None:
                    # avro_extract returned None (unknown wire schema) — record
                    # but don't count toward the per-topic cap, since no data
                    # actually landed.
                    per_topic_skipped[t] = per_topic_skipped.get(t, 0) + 1
                else:
                    per_topic[t] = per_topic.get(t, 0) + 1
                    msgs_seen += 1
                    # Per-topic cap: pause assigned partitions of this topic
                    # so the consumer keeps draining other topics but stops
                    # pulling more of this one this run. Offset will be
                    # snapshotted at the position we paused — next run picks
                    # up cleanly from there.
                    if (max_per_topic > 0
                            and per_topic[t] >= max_per_topic
                            and t not in paused_topics):
                        tps = [tp for tp in consumer.assignment() if tp.topic == t]
                        if tps:
                            consumer.pause(tps)
                        paused_topics.add(t)

                if now - last_flush >= flush_seconds:
                    writers.flush_all()
                    msgs_flushed = msgs_seen
                    last_flush = now

            # If every subscribed topic has hit its cap, exit early — there
            # is no useful work left this run.
            if (max_per_topic > 0
                    and paused_topics
                    and paused_topics.issuperset(topics)):
                stop["flag"] = True
                stop["reason"] = "all-caps-hit"
                break

            # HWM-based early exit: if our position has caught the broker
            # for at least `hwm_grace` seconds straight, stop polling.
            if hwm_grace > 0 and now - last_hwm_check >= hwm_check_seconds:
                last_hwm_check = now
                if is_caught_up(consumer):
                    if caught_up_since is None:
                        caught_up_since = now
                    elif now - caught_up_since >= hwm_grace:
                        stop["flag"] = True
                        stop["reason"] = "caught-up"
                        break
                else:
                    caught_up_since = None
    finally:
        try:
            writers.flush_all()
            msgs_flushed = msgs_seen
            snapshot_offsets(consumer)
        finally:
            writers.close_all()
            consumer.close()

    elapsed = time.monotonic() - start
    print(
        f"done: msgs_seen={msgs_seen} msgs_committed={msgs_flushed} "
        f"elapsed={elapsed:.1f}s reason={stop['reason'] or 'deadline'}"
    )
    for t in topics:
        line = f"  {t}: {per_topic.get(t, 0)} msgs"
        if per_topic_skipped.get(t):
            line += f" ({per_topic_skipped[t]} skipped/unknown-schema)"
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
