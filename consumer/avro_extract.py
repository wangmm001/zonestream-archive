"""Decode Confluent-wire-format Avro payloads from the three OpenINTEL
DarkDNS measurement topics into compact, JSON-serializable extract records.

All three topics share Avro schema id=1 (record `MeasurementResult` in
namespace `nl.openintel.dnspadawan.types.response`; nested `DnsRow` has 116
fields, most usually null). This module keeps the high-value subset useful
for downstream analysis:

  - subject identity         : id, campaign, measurement_node, request_ts
  - per-RR answers (`rows`)  : query_name + response_type + ip4/ip6/ns/mx/
                               cname/ptr, ttl, asn, country, ip_prefix,
                               section_level, authoritative_level, rtt
  - header flags             : ad / aa / tc (union over rows)
  - DNSSEC summary           : counts of rrsig/dnskey/ds/nsec/nsec3 rows
                               (full key/signature wire bytes intentionally
                                dropped — they balloon size 5-10x)
  - policy RRs               : CAA, TLSA preserved in full when present
  - per-row error_message    : surfaced into the row's `err` field

The on-the-wire schema is cached at
``consumer/schemas/avro_id_1.json``; if you need to refresh it, the
authoritative source is the public registry at
``http://schema.zonestream.openintel.nl/subjects/<topic>-value/versions/1``.
"""

from __future__ import annotations

import io
import json
import struct
from pathlib import Path
from typing import Any

from fastavro import parse_schema, schemaless_reader

CONFLUENT_HDR = struct.Struct(">BI")  # 0x00 magic + u32-BE schema id

SCHEMA_PATH = Path(__file__).parent / "schemas" / "avro_id_1.json"

_PARSED_SCHEMA: Any = None


def _schema() -> Any:
    global _PARSED_SCHEMA
    if _PARSED_SCHEMA is None:
        _PARSED_SCHEMA = parse_schema(json.loads(SCHEMA_PATH.read_text()))
    return _PARSED_SCHEMA


def _row(r: dict) -> dict:
    """One DnsRow → compact dict. Keys are short to keep gzipped size down."""
    t = r.get("response_type")
    out: dict[str, Any] = {
        "sec":  r.get("section_level"),
        "auth": r.get("authoritative_level"),
        "q":    r.get("query_name"),
        "qt":   r.get("query_type"),
        "t":    t,
        "ttl":  r.get("response_ttl"),
    }
    ip4 = r.get("ip4_address")
    ip6 = r.get("ip6_address")
    if ip4:
        out["ip4"] = ip4
    if ip6:
        out["ip6"] = ip6
    if t == "NS" and r.get("ns_address"):
        out["ns_name"] = r["ns_address"]
    if t == "MX":
        if r.get("mx_address"):
            out["mx_name"] = r["mx_address"]
        if r.get("mx_preference") is not None:
            out["mx_pref"] = r["mx_preference"]
    if t == "CNAME" and r.get("cname_name"):
        out["target"] = r["cname_name"]
    if t == "DNAME" and r.get("dname_name"):
        out["target"] = r["dname_name"]
    if t == "PTR" and r.get("ptr_name"):
        out["ptr"] = r["ptr_name"]
    if t == "SOA":
        soa = {k2: r.get(k1) for k1, k2 in (
            ("soa_mname", "mname"), ("soa_rname", "rname"),
            ("soa_serial", "serial"), ("soa_refresh", "refresh"),
            ("soa_retry", "retry"), ("soa_expire", "expire"),
            ("soa_minimum", "min"),
        ) if r.get(k1) is not None}
        if soa:
            out["soa"] = soa
    if t == "TXT" and r.get("txt_text"):
        out["txt"] = r["txt_text"]
    if t == "SPF" and r.get("spf_text"):
        out["spf"] = r["spf_text"]
    # IP enrichment is only meaningful when this row resolved to an IP.
    if ip4 or ip6:
        if r.get("as"):
            out["as"] = r["as"]
        if r.get("country"):
            out["country"] = r["country"]
        if r.get("ip_prefix"):
            out["prefix"] = r["ip_prefix"]
    if r.get("rtt") is not None:
        out["rtt"] = r["rtt"]
    if r.get("error_message"):
        out["err"] = r["error_message"]
    return {k: v for k, v in out.items() if v not in (None, "")}


def extract(payload: bytes,
            *,
            ts_ms: int | None = None,
            offset: int | None = None) -> dict | None:
    """Decode one Kafka payload to a compact dict, or None if not schema id=1."""
    if len(payload) < 5 or payload[0] != 0x00:
        return None
    _, schema_id = CONFLUENT_HDR.unpack(payload[:5])
    if schema_id != 1:
        return None
    rec = schemaless_reader(io.BytesIO(payload[5:]), _schema())
    rows = rec.get("resultList") or []

    extracted_rows = [_row(r) for r in rows]

    # DNSSEC summary — counts only; the underlying key material and
    # signatures (`dnskey_pk_*`, `rrsig_signature`) are large and rarely
    # needed at daily-archive granularity.
    counts = {
        "rrsig":  sum(1 for r in rows if r.get("rrsig_algorithm") is not None),
        "dnskey": sum(1 for r in rows if r.get("dnskey_algorithm") is not None),
        "ds":     sum(1 for r in rows if r.get("ds_algorithm") is not None),
        "nsec":   sum(1 for r in rows if r.get("nsec_next_domain_name") is not None),
        "nsec3":  sum(1 for r in rows if r.get("nsec3_hash_algorithm") is not None),
    }

    # CAA + TLSA are small and high-value (CA authorization policy, DANE
    # binding). Keep in full when present.
    caa = [
        {"flags": r.get("caa_flags"), "tag": r.get("caa_tag"), "value": r.get("caa_value")}
        for r in rows if r.get("caa_tag")
    ]
    tlsa = [
        {"q": r.get("query_name"),
         "usage": r.get("tlsa_usage"),
         "selector": r.get("tlsa_selector"),
         "matchtype": r.get("tlsa_matchtype"),
         "certdata": r.get("tlsa_certdata")}
        for r in rows if r.get("tlsa_certdata")
    ]

    flags = {f: any(r.get(f"{f}_flag") for r in rows) for f in ("ad", "aa", "tc")}

    out: dict[str, Any] = {
        "ts_ms":      ts_ms,
        "offset":     offset,
        "id":         rec.get("id"),
        "campaign":   rec.get("campaign"),
        "node":       rec.get("measurement_node"),
        "request_ts": rec.get("request_timestamp"),
        "state":      rec.get("state"),
        "tags":       rec.get("tags") or {},
        "flags":      flags,
        "dnssec":     counts,
        "rows":       extracted_rows,
    }
    if caa:
        out["caa"] = caa
    if tlsa:
        out["tlsa"] = tlsa
    if rec.get("error_message"):
        out["err"] = rec["error_message"]
    # Drop only nulls — keep empty dict/list keys so the shape is stable.
    return {k: v for k, v in out.items() if v is not None}


def to_line(payload: bytes, *, ts_ms: int | None, offset: int | None) -> bytes | None:
    """Convenience: extract + json-encode + trailing newline. None on skip."""
    obj = extract(payload, ts_ms=ts_ms, offset=offset)
    if obj is None:
        return None
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
            + "\n").encode("utf-8")
