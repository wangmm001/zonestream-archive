"""Apply state/offsets.json to the broker.

This is the **only** place broker offsets advance. Run it from the workflow
ONLY after data from the just-finished consume step has been durably pushed
to git. Until this step runs, the broker still believes we are at the
previous run's offsets, so any failure between consume and push leaves the
data available for the next run to re-read.

Idempotent: applying the same offsets twice is harmless.

Env:
- ZS_GROUP_ID  : Kafka consumer group whose offsets to commit
- ZS_BOOTSTRAP : optional, defaults to the public Zonestream broker
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ.get(
    "ZS_BOOTSTRAP", "kafka.zonestream.openintel.nl:9092"
)
STATE_PATH = Path("state/offsets.json")


def main() -> int:
    if not STATE_PATH.exists():
        print(f"{STATE_PATH} not found; nothing to commit", file=sys.stderr)
        return 0

    snap = json.loads(STATE_PATH.read_text())
    snap.pop("_meta", None)

    tps: list[TopicPartition] = []
    for topic, parts in snap.items():
        for p, offset in parts.items():
            tps.append(TopicPartition(topic, int(p), int(offset)))

    if not tps:
        print("no offsets to commit", file=sys.stderr)
        return 0

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": os.environ["ZS_GROUP_ID"],
        "enable.auto.commit": False,
        "security.protocol": "PLAINTEXT",
        "client.id": f"finalize-{os.environ.get('GITHUB_RUN_ID', 'local')}",
        "session.timeout.ms": 30000,
    })

    try:
        # commit() with explicit offsets does not require a subscription —
        # the broker accepts the request as long as the group exists.
        committed = consumer.commit(offsets=tps, asynchronous=False)
        for tp in committed or tps:
            print(f"committed {tp.topic}[{tp.partition}] = {tp.offset}")
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
