import argparse
import csv
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

from codex_sale_docs.open_calgary import (
    _build_subject_queries,
    DEFAULT_DATASET_ID,
    _detect_fields,
    _extract_subject_unit,
    _fetch_rows_paginated,
    _load_field_names,
    _normalize_space,
    _select_subject_row,
    _strip_unit_tokens,
    _subject_where_clauses,
)
from codex_sale_docs.sale_config import load_sale_settings


_SALE_SETTINGS = load_sale_settings()

DEFAULT_SUBJECT_ADDRESS = _SALE_SETTINGS["subject_address"]
DEFAULT_STREET_PORTION = _SALE_SETTINGS["street_portion"]
DEFAULT_OUTPUT_JSON = Path("data/open_calgary_somervale_raw_rows.json")
DEFAULT_OUTPUT_FLAT_CSV = Path("data/open_calgary_somervale_raw_rows_flat.csv")
DEFAULT_OUTPUT_META_JSON = Path("data/open_calgary_somervale_raw_rows_meta.json")
DEFAULT_OUTPUT_FIELD_PROFILE_CSV = Path("data/open_calgary_somervale_raw_field_profile.csv")
DEFAULT_SUBJECT_SEARCH_LIMIT = 200


def _subject_street_portion(subject_address: str) -> str:
    stripped = _strip_unit_tokens(subject_address).split(",")[0]
    match = re.match(r"^\s*\d{3,6}\s+(.+)$", stripped)
    if match:
        return _normalize_space(match.group(1))
    return _normalize_space(stripped)


def _street_portion_from_matched_address(address: str) -> str:
    text = _normalize_space(address).upper()
    if not text:
        return ""
    match = re.match(r"^\s*[A-Z0-9\-]+\s+(\d{3,6}[A-Z]?)\s+(.+)$", text)
    if match:
        return _normalize_space(match.group(2))
    match = re.match(r"^\s*(\d{3,6}[A-Z]?)\s+(.+)$", text)
    if match:
        return _normalize_space(match.group(2))
    return text


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
    subject_search_limit: int,
    max_rows: int,
    street_portion: str,
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

    subject_unit = _extract_subject_unit(subject_address)
    subject_rows: list[dict[str, Any]] = []
    subject_query_urls: list[str] = []
    subject_where_clauses = _subject_where_clauses(subject_address, address_field)
    subject_queries = _build_subject_queries(subject_address)

    lookup_attempts: list[tuple[str, str]] = []
    lookup_attempts.extend([("$where", clause) for clause in subject_where_clauses])
    lookup_attempts.extend([("$q", query) for query in subject_queries])

    for operator, value in lookup_attempts:
        rows, urls = _fetch_rows_paginated(
            dataset_id=dataset_id,
            base_params={operator: value},
            max_rows=max(1, subject_search_limit),
            page_size=page_size,
            app_token=app_token,
            timeout_seconds=timeout_seconds,
        )
        if debug:
            print(f"DEBUG: subject lookup {operator}='{value}' -> {len(rows)} row(s)")
        subject_rows.extend(rows)
        subject_query_urls.extend(urls)
        if rows:
            break

    if not subject_rows:
        raise RuntimeError("No rows returned for subject lookup; cannot derive target street.")

    subject_rows = _dedupe_rows(subject_rows)
    subject_row = _select_subject_row(subject_rows, detected_fields, subject_address, subject_unit)
    matched_subject_address = _normalize_space(str(subject_row.get(address_field, "")).upper())

    street_text = (
        _normalize_space(street_portion).upper()
        or _street_portion_from_matched_address(matched_subject_address)
        or _subject_street_portion(subject_address).upper()
    )
    if not street_text:
        raise RuntimeError("Could not derive street text from subject row.")

    base_clauses = [f"upper({address_field}) like '%{street_text}%'"]

    all_clauses = [*base_clauses, *[item.strip() for item in extra_where_clauses if item.strip()]]
    if not all_clauses:
        raise RuntimeError("No WHERE clauses generated for street fetch.")

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
        "fetch_scope": "subject_street",
        "subject_address_matched": matched_subject_address,
        "subject_lookup_where_clauses": subject_where_clauses,
        "subject_lookup_queries": subject_queries,
        "subject_lookup_query_urls": sorted(set(subject_query_urls)),
        "street_portion": street_text.upper(),
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
            "Fetch City of Calgary open-data rows by first resolving the subject row, "
            "then querying all records on that resolved street, and write raw JSON + flat CSV "
            "artifacts for downstream analysis."
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
    parser.add_argument(
        "--subject-search-limit",
        type=int,
        default=DEFAULT_SUBJECT_SEARCH_LIMIT,
        help="Max rows while resolving the subject row before street expansion.",
    )
    parser.add_argument("--max-rows", type=int, default=60000, help="Maximum rows per where-clause request.")
    parser.add_argument(
        "--street-portion",
        default=DEFAULT_STREET_PORTION,
        help="Optional street-text override. Defaults from matched subject row.",
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
            subject_search_limit=args.subject_search_limit,
            max_rows=args.max_rows,
            street_portion=args.street_portion,
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
    print(f"Fetch scope: {result['fetch_scope']}")
    print(f"Subject address matched: {result['subject_address_matched']}")
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
