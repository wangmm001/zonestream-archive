"""Sample-verify a day's release assets.

For each <topic>-YYYY-MM-DD.tar.zst asset attached to release snap-YYYY-MM-DD:
1. Verify SHA-256 matches the SHA256SUMS body of the release.
2. Decompress, sample SAMPLE_RATE of lines, validate against the per-topic
   JSON Schema in consumer/schemas/.
3. On any failure, exit non-zero so the workflow opens an issue.

Env:
- ZS_DAY       : YYYY-MM-DD (default = yesterday UTC)
- ZS_TOPICS    : comma-separated topic list
- SAMPLE_RATE  : float in (0,1], default 0.01
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import random
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import zstandard as zstd
from jsonschema import Draft202012Validator

SCHEMAS_DIR = Path("consumer/schemas")
WORK_DIR = Path("dist-verify")


def yesterday_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400))


def gh(*args: str) -> str:
    return subprocess.check_output(["gh", *args], text=True)


def fetch_release(tag: str) -> dict[str, Path]:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        ["gh", "release", "download", tag, "--dir", str(WORK_DIR), "--clobber"]
    )
    files: dict[str, Path] = {}
    for p in WORK_DIR.iterdir():
        files[p.name] = p
    return files


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


def iter_jsonl_zst(asset: Path):
    with open(asset, "rb") as f:
        outer = zstd.ZstdDecompressor().stream_reader(f)
        with tarfile.open(fileobj=outer, mode="r|") as tar:
            for member in tar:
                if not member.isfile() or not member.name.endswith(".jsonl.zst"):
                    continue
                inner = tar.extractfile(member)
                if inner is None:
                    continue
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(inner) as r:
                    buf = io.TextIOWrapper(r, encoding="utf-8")
                    for line in buf:
                        line = line.strip()
                        if not line:
                            continue
                        yield member.name, line


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
        asset_name = f"{topic}-{day}.tar.zst"
        asset = files.get(asset_name)
        if asset is None:
            print(f"  - {asset_name}: missing")
            continue
        got = sha256_of(asset)
        want = expected.get(asset_name)
        if want is None or want != got:
            failures.append(f"{asset_name}: sha256 mismatch (want={want} got={got})")
            continue

        validator = load_validator(topic)
        n_total = n_sampled = n_bad = 0
        for member, line in iter_jsonl_zst(asset):
            n_total += 1
            if rng.random() > sample_rate:
                continue
            n_sampled += 1
            try:
                obj = json.loads(line)
            except Exception as exc:
                n_bad += 1
                if n_bad <= 5:
                    failures.append(f"{asset_name}::{member}: invalid JSON ({exc})")
                continue
            if validator is None:
                continue
            errs = list(validator.iter_errors(obj))
            if errs:
                n_bad += 1
                if n_bad <= 5:
                    failures.append(
                        f"{asset_name}::{member}: schema fail ({errs[0].message})"
                    )
        print(f"  + {asset_name}: lines={n_total} sampled={n_sampled} bad={n_bad}")

    if failures:
        print("\nFAIL", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 2
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
