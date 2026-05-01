"""Degraded WebSocket fallback for Zonestream.

WebSocket has no offset/replay, so this consumer is only meant for stretches
when the Kafka path is unavailable. It writes the same hourly NDJSON.zst
layout, tagging every record with `_zs_source = "ws"` so downstream consumers
can dedup against the Kafka-sourced canonical files.

Env:
- ZS_RUN_SECONDS  : wall-clock budget in seconds
- ZS_TOPICS       : comma-separated topic list
- ZS_WS_URL       : override base URL (default wss://zonestream.openintel.nl/ws)
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path

import websockets
import zstandard as zstd

DEFAULT_WS_BASE = "wss://zonestream.openintel.nl/ws"
DATA_ROOT = Path("data")


async def consume_topic(topic: str, base: str, deadline: float, stop: asyncio.Event):
    backoff = 1.0
    while not stop.is_set() and time.monotonic() < deadline:
        url = f"{base.rstrip('/')}/{topic}"
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, max_size=2**24
            ) as ws:
                backoff = 1.0
                async for raw in ws:
                    if stop.is_set() or time.monotonic() >= deadline:
                        break
                    payload = raw.encode() if isinstance(raw, str) else raw
                    write_record(topic, payload)
        except Exception as exc:
            print(f"[ws:{topic}] reconnect after error: {exc}", file=sys.stderr)
            await asyncio.sleep(min(backoff, 30))
            backoff *= 2


def write_record(topic: str, payload: bytes) -> None:
    try:
        obj = json.loads(payload)
    except Exception:
        obj = {"_raw": payload.decode("utf-8", "replace")}
    obj["_zs_source"] = "ws"
    line = (json.dumps(obj, ensure_ascii=False) + "\n").encode()

    hour = time.strftime("%Y/%m/%d/%H", time.gmtime())
    path = DATA_ROOT / topic / f"{hour}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as raw:
        cctx = zstd.ZstdCompressor(level=10)
        with cctx.stream_writer(raw, closefd=False) as w:
            w.write(line)


async def main_async() -> int:
    run_seconds = int(os.environ.get("ZS_RUN_SECONDS", str(5 * 3600 + 45 * 60)))
    topics = [t.strip() for t in os.environ["ZS_TOPICS"].split(",") if t.strip()]
    base = os.environ.get("ZS_WS_URL", DEFAULT_WS_BASE)
    deadline = time.monotonic() + run_seconds

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await asyncio.gather(*(consume_topic(t, base, deadline, stop) for t in topics))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async()))
