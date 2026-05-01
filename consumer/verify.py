"""Sample-verify a day's release assets.

Each release `snap-YYYY-MM-DD` carries one flat artifact per topic
(`<topic>-YYYY-MM-DD.jsonl.gz` for JSON topics, `<topic>-YYYY-MM-DD.kfb.zst`
for binary ones — but binary topics are not in the default set) plus a
`SHA256SUMS` file. We:

1. Download all assets to `dist-verify/`.
2. Verify each asset's SHA-256 matches `SHA256SUMS`.
3. For JSON assets, sample-validate against the per-topic JSON Schema.
4. For binary assets, sample-validate the Confluent framing magic byte.

Exits non-zero if any check fails so the workflow can open an issue.

Env:
- ZS_DAY      : YYYY-MM-DD (default = yesterday UTC)
- ZS_TOPICS   : comma-separated topic list
- SAMPLE_RATE : float in (0,1], default 0.01
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import random
import struct
import subprocess
import sys
import time
from pathlib import Path

import zstandard as zstd
from jsonschema import Draft202012Validator

SCHEMAS_DIR = Path("consumer/schemas")
WORK_DIR = Path("dist-verify")
_KFB_HDR = struct.Struct(">QI")


def yesterday_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400))


def fetch_release(tag: str) -> dict[str, Path]:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        ["gh", "release", "download", tag, "--dir", str(WORK_DIR), "--clobber"]
    )
    return {p.name: p for p in WORK_DIR.iterdir()}


def parse_sumfile(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        out[parts[-1]] = parts[0]
    return out


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def iter_jsonl_gz(asset: Path):
    """Yield (line_number, line) for each non-blank line."""
    with gzip.open(asset, "rt", encoding="utf-8") as f:
        for ln_no, line in enumerate(f, 1):
            line = line.strip()
            if line:
                yield ln_no, line


def iter_kfb_zst(asset: Path):
    """Yield (frame_index, ts_ms, payload) for each .kfb.zst frame."""
    idx = 0
    with open(asset, "rb") as f:
        with zstd.ZstdDecompressor().stream_reader(f) as r:
            while True:
                head = r.read(_KFB_HDR.size)
                if not head:
                    return
                if len(head) < _KFB_HDR.size:
                    raise ValueError("truncated frame header")
                ts_ms, ln = _KFB_HDR.unpack(head)
                payload = r.read(ln)
                if len(payload) != ln:
                    raise ValueError("truncated frame payload")
                idx += 1
                yield idx, ts_ms, payload


def load_validator(topic: str) -> Draft202012Validator | None:
    path = SCHEMAS_DIR / f"{topic}.json"
    if not path.exists():
        return None
    return Draft202012Validator(json.loads(path.read_text()))


def main() -> int:
    day = os.environ.get("ZS_DAY") or yesterday_utc()
    topics = [t.strip() for t in os.environ["ZS_TOPICS"].split(",") if t.strip()]
    sample_rate = float(os.environ.get("SAMPLE_RATE", "0.01"))
    tag = f"snap-{day}"

    print(f"verifying {tag}")
    try:
        files = fetch_release(tag)
    except subprocess.CalledProcessError as exc:
        print(f"release {tag} not found: {exc}", file=sys.stderr)
        return 1

    if "SHA256SUMS" not in files:
        print("SHA256SUMS missing from release", file=sys.stderr)
        return 1
    expected = parse_sumfile(files["SHA256SUMS"].read_text())

    failures: list[str] = []
    rng = random.Random(0xC0FFEE ^ hash(day))

    for topic in topics:
        # JSON asset name
        json_name = f"{topic}-{day}.jsonl.gz"
        kfb_name = f"{topic}-{day}.kfb.zst"
        if json_name in files:
            asset = files[json_name]
            asset_name = json_name
            mode = "json"
        elif kfb_name in files:
            asset = files[kfb_name]
            asset_name = kfb_name
            mode = "binary"
        else:
            print(f"  - {topic}: no asset for {day}")
            continue

        got = sha256_of(asset)
        want = expected.get(asset_name)
        if want is None or want != got:
            failures.append(f"{asset_name}: sha256 mismatch (want={want} got={got})")
            continue

        n_total = n_sampled = n_bad = 0
        try:
            if mode == "json":
                validator = load_validator(topic)
                for ln_no, line in iter_jsonl_gz(asset):
                    n_total += 1
                    if rng.random() > sample_rate:
                        continue
                    n_sampled += 1
                    try:
                        obj = json.loads(line)
                    except Exception as exc:
                        n_bad += 1
                        if n_bad <= 5:
                            failures.append(f"{asset_name}:{ln_no}: invalid JSON ({exc})")
                        continue
                    if validator is None:
                        continue
                    errs = list(validator.iter_errors(obj))
                    if errs:
                        n_bad += 1
                        if n_bad <= 5:
                            failures.append(
                                f"{asset_name}:{ln_no}: schema fail "
                                f"({errs[0].message})"
                            )
            else:  # binary
                for idx, ts_ms, payload in iter_kfb_zst(asset):
                    n_total += 1
                    if rng.random() > sample_rate:
                        continue
                    n_sampled += 1
                    if len(payload) < 5 or payload[0] != 0x00:
                        n_bad += 1
                        if n_bad <= 5:
                            failures.append(
                                f"{asset_name} frame {idx}: bad Confluent magic "
                                f"(first byte 0x{payload[:1].hex()})"
                            )
        except (ValueError, OSError) as exc:
            failures.append(f"{asset_name}: read error: {exc}")

        kind = "frames" if mode == "binary" else "lines"
        print(f"  + {asset_name}: {kind}={n_total} sampled={n_sampled} bad={n_bad}")

    if failures:
        print("\nFAIL", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 2
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
