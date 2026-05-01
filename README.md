# zonestream-archive

Long-term archive of [OpenINTEL Zonestream](https://openintel.nl/data/zonestream/)
(real-time DNS zone updates, see *DarkDNS: Revisiting the Value of Rapid Zone
Update*, IMC '24) running entirely on free GitHub resources.

Data is organised the same way as the sibling
[`wangmm001/top-domains-aggregate`](https://github.com/wangmm001/top-domains-aggregate):
**one flat per-day file plus a `current` pointer**, except split per topic.

## Layout

```
data/
  <topic>/
    YYYY-MM-DD.jsonl.gz       # finalized daily file (after rollover)
    current.jsonl.gz          # byte copy of the most recent day
    _partial/                 # transient hourly fragments for the current day,
      YYYY-MM-DD/             # eaten by the next 00:10 UTC rollover
        HH-r<runid>.jsonl.zst
```

`<topic>` is one of `newly_registered_domain`, `newly_registered_fqdn`,
`confirmed_newly_registered_domain` (the three lean defaults). Each line is
one JSON document straight from the Kafka broker — see
[Topics](#topics) below for the per-topic schemas.

## Pipeline

A single `daily.yml` workflow runs once per UTC day and chains three jobs:

```
Zonestream (Kafka :9092 anon / WebSocket /ws)
        │
        ▼  daily.yml @ 00:30 UTC  (≈3 min total)
┌─ consume   drain everything since last run, dedup-friendly write to
│            data/<topic>/_partial/<day>/<HH>-r<run>.jsonl.zst.
│            broker offsets are NOT advanced until git push succeeds —
│            see consumer/finalize_offsets.py
│
├─ rollover  for each topic: pack _partial/<yesterday>/*.jsonl.zst →
│            data/<topic>/<yesterday>.jsonl.gz (deterministic gzip,
│            line-bytes dedup) → copy → current.jsonl.gz →
│            upload as Release asset → git rm _partial/<yesterday>/
│            → refresh INDEX.md
│
└─ verify    sha256-check + JSON-Schema-sample-validate the release
             we just produced; opens an issue on failure

reaper.yml hourly — opens an issue if no data commit lands for 36 h
```

### Why daily

Live message rate of the three default topics combined is ~5 records/s
(~450 K/day). A single 10-minute drain comfortably covers ~24 h of backlog
even with every-day-fails-for-a-week recovery. Kafka retains ~28 days, so
even multiple consecutive failed days lose nothing — the next successful
run reads the missing window from the broker.

Real cost: `current.jsonl.gz` lags by up to ~24 h after the latest record.
That's the only thing the daily cadence trades away.

## Topics

Eight topics live on the public broker (`kafka.zonestream.openintel.nl:9092`,
anonymous). Only the three small JSON ones are archived by default — the
other five together produce ~49 GiB/day, which does not fit on GitHub.

| topic                                   | format | day volume | default |
|-----------------------------------------|--------|-----------:|---------|
| `newly_registered_domain`               | JSON   | ~11 MB     | ✅       |
| `newly_registered_fqdn`                 | JSON   | ~12 MB     | ✅       |
| `confirmed_newly_registered_domain`     | JSON   | ~1 MB      | ✅       |
| `certstream`                            | JSON   | ~15 GB     | —        |
| `certstream_domains`                    | JSON   | ~8.6 GB    | —        |
| `newly_issued_certificates_measurements`| Avro   | ~9 GB      | —        |
| `newly_registered_domains_measurements` | Avro   | ~9.7 GB    | —        |
| `newly_registered_fqdn_measurements`    | Avro   | ~8.4 GB    | —        |

JSON topics land as `*.jsonl.zst` (one record per line). Avro-on-the-wire
topics use Confluent framing (`0x00 || u32-BE schema_id || avro_payload`) and
are stored as `*.kfb.zst` — see [KAFKA_FRAMING.md](KAFKA_FRAMING.md). Schemas
under `consumer/schemas/` are JSON Schema for the JSON topics; Avro schemas
are not yet published by OpenINTEL, so binary payloads are preserved verbatim
for later replay.

To temporarily enable a high-volume topic, dispatch `consume.yml` with the
`topics` input. To make a long-term change, set `vars.ZS_TOPICS` in repo
settings — but plan for object storage offload (S3/R2/B2) before doing so;
this archive's git+release path tops out around ~50 MB/day.

## Setup

Zonestream's Kafka stream is anonymous (`kafka.zonestream.openintel.nl:9092`,
`PLAINTEXT`), so there is no credential setup at all.

1. Make the repo **public** to get unlimited Actions minutes.
2. *Optional*: in repo settings → *Variables* (not Secrets), set
   `ZS_GROUP_ID` to a stable string. This is just a label the broker uses to
   remember your consumer offsets — pick something unique like
   `gha-archive-<your-handle>`. If unset, a default of
   `gha-archive-<repository_id>` is used.
3. Smoke test: *Actions* → `consume` → `Run workflow` with
   `run_seconds=600`.
4. Once a clean batch lands, the cron schedules take over.

## Consume

```bash
# Today's newly registered domains, latest snapshot
curl -L https://raw.githubusercontent.com/wangmm001/zonestream-archive/main/data/newly_registered_domain/current.jsonl.gz \
  | zcat | head

# Yesterday's full DarkDNS-validated list
gh release download snap-2026-04-30 -R wangmm001/zonestream-archive \
  -p 'confirmed_newly_registered_domain-2026-04-30.jsonl.gz' -p 'SHA256SUMS'
sha256sum -c SHA256SUMS
zcat confirmed_newly_registered_domain-2026-04-30.jsonl.gz | wc -l
```

## Caveats

- WebSocket has no offset; it is only a degraded fallback.
- Schemas under `consumer/schemas/` are intentionally permissive; tighten as
  you learn the actual payload distribution.
- `INDEX.md` is regenerated by `rollover.yml`; do not hand-edit.
