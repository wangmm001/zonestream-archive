"""Force-reset broker-committed offsets for our consumer group to the broker
low-water-mark on every assigned partition.

Use this once before a backfill that needs to re-read everything still
retained on the broker (typical Zonestream retention is ~28 days).

After this script runs, the next consume job will see no committed offsets
above the low-water-mark and start draining from the beginning.

Env:
- ZS_GROUP_ID  : consumer group whose offsets to reset
- ZS_TOPICS    : comma-separated topics (we reset all partitions of each)
- ZS_BOOTSTRAP : optional, defaults to public Zonestream broker
"""

from __future__ import annotations

import os
import sys
import time

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ.get(
    "ZS_BOOTSTRAP", "kafka.zonestream.openintel.nl:9092"
)


def main() -> int:
    group_id = os.environ["ZS_GROUP_ID"]
    topics = [t.strip() for t in os.environ["ZS_TOPICS"].split(",") if t.strip()]

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "security.protocol": "PLAINTEXT",
        "client.id": f"reset-{os.environ.get('GITHUB_RUN_ID', 'local')}",
        "session.timeout.ms": 30000,
    })

    md = c.list_topics(timeout=15)
    tps: list[TopicPartition] = []
    for topic in topics:
        if topic not in md.topics:
            print(f"warn: topic {topic} not in broker metadata", file=sys.stderr)
            continue
        for p in md.topics[topic].partitions:
            low, high = c.get_watermark_offsets(
                TopicPartition(topic, p), timeout=10
            )
            print(
                f"  {topic}[{p}]: broker=[{low}..{high}] → "
                f"resetting committed offset to {low}"
            )
            tps.append(TopicPartition(topic, p, low))

    if not tps:
        print("nothing to reset")
        return 0

    c.commit(offsets=tps, asynchronous=False)
    print(
        f"\nreset {len(tps)} partitions in group '{group_id}' "
        f"to low-water-marks @ {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
    )
    c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
