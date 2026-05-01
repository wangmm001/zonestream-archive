"""Compact `data/<topic>/_partial/<YYYY-MM-DD>/` fragments into one flat
per-day artifact.

JSON topics:
  Reads every ``HH-r*.jsonl.zst`` for the given day, decompresses, sorts by
  filename (so ordering is stable: hour ascending, run-id ascending), strips
  blank lines, and writes ``data/<topic>/<YYYY-MM-DD>.jsonl.gz`` plus a
  byte-identical ``data/<topic>/current.jsonl.gz``. Reproducible gzip header
  (mtime=0) so re-running the script on identical input produces an
  identical file.

Binary topics:
  Concatenates ``HH-r*.kfb.zst`` byte-for-byte (zstd is multi-frame
  compatible, so concatenated frames decompress to the concatenation).
  Writes ``data/<topic>/<YYYY-MM-DD>.kfb.zst`` plus ``current.kfb.zst``.

Prints the path of the produced artifact so the calling workflow can pick
it up. Exits 0 with no output if there is no work for the day.

Usage:
  pack_day.py <topic> <YYYY-MM-DD>
"""

from __future__ import annotations

import gzip
import hashlib
import os
import shutil
import sys
from pathlib import Path

import zstandard as zstd

DATA_ROOT = Path("data")


def pack_json(topic: str, day: str) -> Path | None:
    src = DATA_ROOT / topic / "_partial" / day
    if not src.is_dir():
        return None
    fragments = sorted(src.glob("*.jsonl.zst"))
    if not fragments:
        return None
    out = DATA_ROOT / topic / f"{day}.jsonl.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    dctx = zstd.ZstdDecompressor()
    # Reproducible gzip: mtime=0, no filename.
    with open(out, "wb") as raw, gzip.GzipFile(
        fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=9
    ) as gz:
        for frag in fragments:
            with open(frag, "rb") as f, dctx.stream_reader(f) as r:
                # Copy chunked to keep memory bounded.
                while True:
                    buf = r.read(1 << 20)
                    if not buf:
                        break
                    # Skip blank lines that may slip through partials.
                    if b"\n\n" in buf:
                        buf = b"\n".join(
                            ln for ln in buf.split(b"\n") if ln.strip()
                        )
                        if buf:
                            buf += b"\n"
                    gz.write(buf)
    _update_current(topic, out)
    return out


def pack_binary(topic: str, day: str) -> Path | None:
    src = DATA_ROOT / topic / "_partial" / day
    if not src.is_dir():
        return None
    fragments = sorted(src.glob("*.kfb.zst"))
    if not fragments:
        return None
    out = DATA_ROOT / topic / f"{day}.kfb.zst"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as o:
        for frag in fragments:
            with open(frag, "rb") as f:
                shutil.copyfileobj(f, o, length=1 << 20)
    _update_current(topic, out)
    return out


def _update_current(topic: str, daily: Path) -> None:
    suffix = "".join(daily.suffixes)
    current = DATA_ROOT / topic / f"current{suffix}"
    # Bytewise copy so cloners don't need symlink support.
    shutil.copyfile(daily, current)


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: pack_day.py <topic> <YYYY-MM-DD>", file=sys.stderr)
        return 2
    topic, day = argv[1], argv[2]
    out = pack_json(topic, day) or pack_binary(topic, day)
    if out is None:
        print(f"# no fragments for {topic}/{day}", file=sys.stderr)
        return 0
    sz = out.stat().st_size
    print(f"{out}\t{sz}\t{sha256(out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
