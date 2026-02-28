import argparse
import csv
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

from codex_sale_docs.open_calgary import (
    DEFAULT_DATASET_ID,
    _detect_fields,
    _fetch_rows_paginated,
    _load_field_names,
    _normalize_space,
    _strip_unit_tokens,
)


DEFAULT_SUBJECT_ADDRESS = "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2"
DEFAULT_OUTPUT_JSON = Path("data/open_calgary_somervale_raw_rows.json")
DEFAULT_OUTPUT_FLAT_CSV = Path("data/open_calgary_somervale_raw_rows_flat.csv")
DEFAULT_OUTPUT_META_JSON = Path("data/open_calgary_somervale_raw_rows_meta.json")
DEFAULT_OUTPUT_FIELD_PROFILE_CSV = Path("data/open_calgary_somervale_raw_field_profile.csv")
DEFAULT_STREET_TYPE_ALIASES = "CO,COURT,CT"


def _subject_street_portion(subject_address: str) -> str:
    stripped = _strip_unit_tokens(subject_address).split(",")[0]
    match = re.match(r"^\s*\d{3,6}\s+(.+)$", stripped)
    if match:
        return _normalize_space(match.group(1))
    return _normalize_space(stripped)


def _street_components(street_portion: str) -> tuple[str, str]:
    tokens = [token for token in re.findall(r"[A-Z0-9]+", street_portion.upper()) if not token.isdigit()]
    if not tokens:
        return "", ""
    direction_tokens = {"N", "S", "E", "W", "NE", "NW", "SE", "SW", "AB", "CALGARY"}
    tokens = [token for token in tokens if token not in direction_tokens]
    if not tokens:
        return "", ""
    street_name = tokens[0]
    street_type = tokens[1] if len(tokens) >= 2 else ""
    return street_name, street_type


def _street_type_aliases(street_type: str, overrides_csv: str) -> list[str]:
    if overrides_csv.strip():
        aliases = [item.strip().upper() for item in overrides_csv.split(",") if item.strip()]
        return aliases

    default_aliases = {
        "COURT": ["CO", "CT", "COURT"],
        "CO": ["CO", "CT", "COURT"],
        "CT": ["CO", "CT", "COURT"],
        "STREET": ["ST", "STREET"],
        "ST": ["ST", "STREET"],
        "AVENUE": ["AV", "AVE", "AVENUE"],
        "AV": ["AV", "AVE", "AVENUE"],
        "AVE": ["AV", "AVE", "AVENUE"],
        "ROAD": ["RD", "ROAD"],
        "RD": ["RD", "ROAD"],
        "DRIVE": ["DR", "DRIVE"],
        "DR": ["DR", "DRIVE"],
        "BOULEVARD": ["BLVD", "BV", "BOULEVARD"],
        "BV": ["BLVD", "BV", "BOULEVARD"],
        "BLVD": ["BLVD", "BV", "BOULEVARD"],
    }
    aliases = default_aliases.get(street_type.upper(), [street_type.upper()] if street_type else [])
    seen: set[str] = set()
    ordered: list[str] = []
    for alias in aliases:
        if alias and alias not in seen:
            seen.add(alias)
            ordered.append(alias)
    return ordered


def _street_where_clauses(
    *,
    street_portion: str,
    address_field: str,
    street_type_aliases_csv: str,
    include_street_name_only: bool,
) -> list[str]:
    if not street_portion or not address_field:
        return []
    street_name, street_type = _street_components(street_portion)
    if not street_name:
        return []
    aliases = _street_type_aliases(street_type, street_type_aliases_csv)
    clauses: list[str] = []
    for alias in aliases:
        clauses.append(f"upper({address_field}) like '%{street_name} {alias}%'")
    if include_street_name_only or not clauses:
        clauses.append(f"upper({address_field}) like '%{street_name}%'")
    return clauses


def _row_dedupe_key(row: dict[str, Any]) -> str:
    unique_key = str(row.get("unique_key", "")).strip()
    if unique_key:
        return f"unique_key:{unique_key}"
    roll = str(row.get("roll_number", "")).strip()
    if roll:
        return f"roll_number:{roll}"
    address = str(row.get("address", "")).strip().upper()
    if address:
        return f"address:{address}|raw:{json.dumps(row, sort_keys=True)}"
    return f"raw:{json.dumps(row, sort_keys=True)}"


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = _row_dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_flat_csv(path: Path, rows: list[dict[str, Any]]) -> list[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return []
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            flattened: dict[str, Any] = {}
            for field in fieldnames:
                value = row.get(field, "")
                if isinstance(value, (dict, list)):
                    flattened[field] = json.dumps(value, separators=(",", ":"))
                else:
                    flattened[field] = value
            writer.writerow(flattened)
    return fieldnames


def _write_field_profile(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    field_counts: dict[str, int] = {}
    total = len(rows)
    for row in rows:
        for key, value in row.items():
            if value is None:
                continue
            if str(value).strip() == "":
                continue
            field_counts[key] = field_counts.get(key, 0) + 1
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["field", "present_count", "present_pct"])
        writer.writeheader()
        for field in sorted(field_counts):
            count = field_counts[field]
            pct = (count / total * 100) if total else 0.0
            writer.writerow({"field": field, "present_count": count, "present_pct": f"{pct:.2f}"})


def fetch_city_data(
    *,
    subject_address: str,
    dataset_id: str,
    app_token: str,
    timeout_seconds: int,
    page_size: int,
    max_rows: int,
    street_portion: str,
    street_type_aliases_csv: str,
    include_street_name_only: bool,
    extra_where_clauses: list[str],
    address_field_override: str,
    dedupe: bool,
    output_json: Path,
    output_flat_csv: Path,
    output_meta_json: Path,
    output_field_profile_csv: Path,
    debug: bool,
) -> dict[str, Any]:
    field_names = _load_field_names(dataset_id, app_token, timeout_seconds)
    detected_fields = _detect_fields(field_names)
    address_field = address_field_override.strip() or detected_fields.get("address", "")
    if not address_field:
        raise RuntimeError("Could not detect address field for the dataset.")

    street_text = _normalize_space(street_portion) or _subject_street_portion(subject_address)
    base_clauses = _street_where_clauses(
        street_portion=street_text,
        address_field=address_field,
        street_type_aliases_csv=street_type_aliases_csv,
        include_street_name_only=include_street_name_only,
    )
    all_clauses = [*base_clauses, *[item.strip() for item in extra_where_clauses if item.strip()]]
    if not all_clauses:
        raise RuntimeError("No WHERE clauses generated; provide --street-portion and/or --extra-where-clause.")

    all_rows: list[dict[str, Any]] = []
    query_urls: list[str] = []
    for clause in all_clauses:
        rows, urls = _fetch_rows_paginated(
            dataset_id=dataset_id,
            base_params={"$where": clause},
            max_rows=max_rows,
            page_size=page_size,
            app_token=app_token,
            timeout_seconds=timeout_seconds,
        )
        all_rows.extend(rows)
        query_urls.extend(urls)
        if debug:
            print(f"DEBUG: $where='{clause}' -> {len(rows)} row(s)")

    rows_raw_count = len(all_rows)
    rows_filtered = all_rows
    rows_deduped = _dedupe_rows(rows_filtered) if dedupe else rows_filtered

    run_timestamp = dt.datetime.now(dt.timezone.utc)
    run_id = run_timestamp.strftime("%Y%m%dT%H%M%SZ")
    _write_json(output_json, rows_deduped)
    csv_fields = _write_flat_csv(output_flat_csv, rows_deduped)
    _write_field_profile(output_field_profile_csv, rows_deduped)

    meta_payload = {
        "dataset_id": dataset_id,
        "run_id": run_id,
        "captured_at": run_timestamp.isoformat(),
        "subject_address": subject_address,
        "street_portion": street_text,
        "address_field": address_field,
        "detected_fields": detected_fields,
        "where_clauses": all_clauses,
        "query_urls": sorted(set(query_urls)),
        "rows_raw": rows_raw_count,
        "rows_filtered": len(rows_filtered),
        "rows_deduped": len(rows_deduped),
        "flat_csv_fieldnames": csv_fields,
        "dedupe_enabled": dedupe,
    }
    _write_json(output_meta_json, meta_payload)
    return meta_payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch City of Calgary open-data rows for all records on a target street and "
            "write raw JSON + flat CSV artifacts for downstream analysis."
        )
    )
    parser.add_argument("--subject-address", default=DEFAULT_SUBJECT_ADDRESS, help="Subject address used for defaults.")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID, help="Socrata dataset id.")
    parser.add_argument(
        "--app-token",
        default=os.environ.get("SOCRATA_APP_TOKEN", ""),
        help="Socrata app token (defaults to SOCRATA_APP_TOKEN).",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--page-size", type=int, default=1000, help="Pagination page size.")
    parser.add_argument("--max-rows", type=int, default=60000, help="Maximum rows per where-clause request.")
    parser.add_argument(
        "--street-portion",
        default="",
        help="Street portion override (for example: 'Somervale Court SW'). Defaults from subject address.",
    )
    parser.add_argument(
        "--street-type-aliases",
        default=DEFAULT_STREET_TYPE_ALIASES,
        help="Comma-delimited street type aliases (default: CO,COURT,CT).",
    )
    parser.add_argument(
        "--include-street-name-only",
        action="store_true",
        help="Also include a broad clause matching just the street name token.",
    )
    parser.add_argument(
        "--extra-where-clause",
        action="append",
        default=[],
        help="Additional raw SoQL $where clause (repeatable).",
    )
    parser.add_argument("--address-field", default="", help="Optional address field override.")
    parser.add_argument("--no-dedupe", action="store_true", help="Disable deduplication across fetched rows.")
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON), help="Output raw JSON path.")
    parser.add_argument("--output-flat-csv", default=str(DEFAULT_OUTPUT_FLAT_CSV), help="Output flat CSV path.")
    parser.add_argument("--output-meta-json", default=str(DEFAULT_OUTPUT_META_JSON), help="Output metadata JSON path.")
    parser.add_argument(
        "--output-field-profile-csv",
        default=str(DEFAULT_OUTPUT_FIELD_PROFILE_CSV),
        help="Output field profile CSV path.",
    )
    parser.add_argument("--debug", action="store_true", help="Print debug details.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = fetch_city_data(
            subject_address=_normalize_space(args.subject_address),
            dataset_id=args.dataset_id,
            app_token=args.app_token,
            timeout_seconds=args.timeout_seconds,
            page_size=args.page_size,
            max_rows=args.max_rows,
            street_portion=args.street_portion,
            street_type_aliases_csv=args.street_type_aliases,
            include_street_name_only=bool(args.include_street_name_only),
            extra_where_clauses=list(args.extra_where_clause),
            address_field_override=args.address_field,
            dedupe=not bool(args.no_dedupe),
            output_json=Path(args.output_json),
            output_flat_csv=Path(args.output_flat_csv),
            output_meta_json=Path(args.output_meta_json),
            output_field_profile_csv=Path(args.output_field_profile_csv),
            debug=bool(args.debug),
        )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1

    print(f"Dataset: {result['dataset_id']}")
    print(f"Street portion: {result['street_portion']}")
    print(f"Address field: {result['address_field']}")
    print(f"Rows raw: {result['rows_raw']}")
    print(f"Rows deduped: {result['rows_deduped']}")
    print(f"Wrote raw JSON: {args.output_json}")
    print(f"Wrote flat CSV: {args.output_flat_csv}")
    print(f"Wrote metadata JSON: {args.output_meta_json}")
    print(f"Wrote field profile CSV: {args.output_field_profile_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
