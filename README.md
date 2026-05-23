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
`confirmed_newly_registered_domain` (the three lean defaults), or — opt-in —
one of the three `*_measurements` topics, in which case each line is a
**compact JSON extract** decoded from the upstream Avro (see
[Avro measurement topics](#avro-measurement-topics) below). For the JSON
defaults, each line is one document straight from the Kafka broker — see
[Topics](#topics) for the per-topic schemas.

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

| topic                                   | format         | upstream day vol | default |
|-----------------------------------------|----------------|----------------:|---------|
| `newly_registered_domain`               | JSON           | ~11 MB           | ✅       |
| `newly_registered_fqdn`                 | JSON           | ~12 MB           | ✅       |
| `confirmed_newly_registered_domain`     | JSON           | ~1 MB            | ✅       |
| `certstream`                            | JSON           | ~15 GB           | —        |
| `certstream_domains`                    | JSON           | ~8.6 GB          | —        |
| `newly_issued_certificates_measurements`| `avro_extract` | ~9 GB            | —        |
| `newly_registered_domains_measurements` | `avro_extract` | ~9.7 GB          | —        |
| `newly_registered_fqdn_measurements`    | `avro_extract` | ~8.4 GB          | —        |

JSON topics land as `*.jsonl.zst` (one record per line). Topics with format
`avro_extract` decode the upstream Confluent-wire Avro payload (schema id=1,
record `MeasurementResult`) with a bundled schema at
`consumer/schemas/avro_id_1.json` and write a compact JSON extract to
`*.jsonl.zst` — see [Avro measurement topics](#avro-measurement-topics). If
you instead want lossless raw frames (`*.kfb.zst`), set the topic's
`TOPIC_FORMAT` entry in `consumer/kafka_consumer.py` to `"binary"` and read
back with the snippet in [KAFKA_FRAMING.md](KAFKA_FRAMING.md).

To temporarily enable an opt-in topic, dispatch `daily.yml` with the `topics`
input. To make a long-term change, set `vars.ZS_TOPICS` in repo settings.
For the three `*_measurements` topics, **also set `vars.ZS_MAX_MSGS_PER_TOPIC`**
to bound the daily release size — see the budget table below.

### Avro measurement topics

The three `*_measurements` topics carry the per-query DNS results that
DarkDNS's measurement workers (one host in Enschede with 16 workers, contrary
to the IMC '24 paper) produce for newly-registered domains / FQDNs /
CT-newly-issued certificates. All three share Avro schema id=1
(`nl.openintel.dnspadawan.types.response.MeasurementResult` with a 116-field
nested `DnsRow`). Live throughput is ~3000 msg/s peak on the largest topic.

A daily `consume` run cannot drain the full ~27 GB/day uncompressed. We
instead pull a **bounded sample** per topic (cap with
`ZS_MAX_MSGS_PER_TOPIC`) and decode each message into a compact extract
keeping:

- subject identity (`id`, `campaign`, `node`, `request_ts`, `tags`)
- per-RR answers (`rows[]`): query name + type, `ip4`/`ip6`/`ns_name`/
  `mx_name`/`target`/`ptr`/`soa`/`txt`/`spf`, `ttl`, `as`/`country`/`prefix`,
  `rtt`, `section_level`, `authoritative_level`
- header flag union (`ad`/`aa`/`tc`)
- DNSSEC presence counts (`rrsig`/`dnskey`/`ds`/`nsec`/`nsec3` — the full
  key/signature wire bytes are dropped; counts are enough for deployment
  inventories)
- CAA + TLSA records (kept in full when present)

Size budget (measured on 10k real records from `newly_registered_domains_measurements`):

| messages | raw Avro | extract JSON (uncompressed) | gzipped (level 9) | zstd (level 10) |
|---------:|---------:|----------------------------:|------------------:|----------------:|
|  10 000  | 21.6 MB  | 17.6 MB                     | 1.3 MB            | 0.71 MB         |
|  50 000  | ~108 MB  | ~88 MB                      | ~6.5 MB           | ~3.6 MB         |
| 100 000  | ~216 MB  | ~176 MB                     | ~13 MB            | ~7 MB           |

Recommended starting point: `ZS_MAX_MSGS_PER_TOPIC=50000` → ~20 MB/day of
gzipped releases across the three measurement topics combined. Raise as your
release storage permits. The full JSON Schema for one extract record is at
[`consumer/schemas/_measurement_extract.json`](consumer/schemas/_measurement_extract.json).

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
