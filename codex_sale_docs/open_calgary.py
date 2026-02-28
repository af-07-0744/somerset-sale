import argparse
from collections import Counter
import csv
import datetime as dt
import getpass
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://data.calgary.ca"
RESOURCE_URL_TEMPLATE = BASE_URL + "/resource/{dataset_id}.json"
VIEWS_URL_TEMPLATE = BASE_URL + "/api/views/{dataset_id}.json"

DEFAULT_DATASET_ID = "4bsw-nn7w"
DEFAULT_OUTPUT_CSV = Path("data/open_calgary_assessment_comps.csv")
DEFAULT_SOURCE_REGISTRY = Path("data/source_registry.csv")
DEFAULT_COMPS_RAW = Path("data/comps_raw.csv")
DEFAULT_EVIDENCE_DIR = Path("evidence/open_calgary")

COMPS_RAW_FIELDNAMES = [
    "comp_id",
    "address",
    "unit",
    "status",
    "list_price",
    "sold_price",
    "sale_date",
    "sqft",
    "bedrooms",
    "bathrooms",
    "parking",
    "dom",
    "condition",
    "origin_notes",
]

SOURCE_REGISTRY_FIELDNAMES = [
    "source_id",
    "comp_id",
    "source_type",
    "mls_number",
    "url",
    "publisher",
    "captured_at",
    "captured_by",
    "file_path",
    "file_sha256",
    "claims_supported",
    "notes",
]

ASSESSMENT_COMPS_FIELDNAMES = [
    "comp_id",
    "source_id",
    "address",
    "unit",
    "community",
    "assessed_value",
    "assessment_year",
    "property_type",
    "sqft",
    "roll_number",
    "value_delta",
    "value_delta_pct",
    "dataset_id",
    "source_url",
]


def _parse_csv_set(raw_value: str) -> set[str]:
    parts = [item.strip().upper() for item in raw_value.split(",")]
    return {item for item in parts if item}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw_value = str(value).strip()
    if not raw_value:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", raw_value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _strip_unit_tokens(address: str) -> str:
    stripped = re.sub(r"#\s*\w+", " ", address, flags=re.IGNORECASE)
    stripped = re.sub(r"\b(unit|suite|apt|apartment)\s*\w+\b", " ", stripped, flags=re.IGNORECASE)
    return _normalize_space(stripped).strip(",")


def _build_subject_queries(address: str) -> list[str]:
    def normalize_for_search(raw_text: str) -> str:
        normalized = _normalize_space(raw_text)
        replacements = {
            " avenue ": " av ",
            " street ": " st ",
            " road ": " rd ",
            " boulevard ": " bv ",
            " drive ": " dr ",
            " court ": " ct ",
            " southwest ": " sw ",
            " southeast ": " se ",
            " northwest ": " nw ",
            " northeast ": " ne ",
        }
        candidate = f" {normalized.lower()} "
        for source, target in replacements.items():
            candidate = candidate.replace(source, target)
        return _normalize_space(candidate.upper())

    queries: list[str] = []
    stripped = _strip_unit_tokens(address)
    local_only = stripped.split(",")[0]
    tokenized = re.sub(r"[^A-Za-z0-9 ]", " ", local_only).split()
    short_core = " ".join(tokenized[:3]) if tokenized else ""
    candidates = [
        address,
        stripped,
        local_only,
        normalize_for_search(local_only),
        short_core,
        normalize_for_search(short_core),
    ]

    for candidate in candidates:
        normalized = _normalize_space(candidate)
        if normalized and normalized not in queries:
            queries.append(normalized)
    return queries


def _extract_subject_unit(address: str) -> str:
    hash_match = re.search(r"#\s*([A-Za-z0-9\-]+)", address)
    if hash_match:
        return re.sub(r"[^A-Za-z0-9]", "", hash_match.group(1)).upper()
    leading_unit_match = re.match(r"^\s*([A-Za-z0-9\-]+)\s+\d{3,5}\b", address)
    if leading_unit_match:
        return re.sub(r"[^A-Za-z0-9]", "", leading_unit_match.group(1)).upper()
    return ""


def _extract_suite_token_from_address(address_text: str) -> str:
    tokens = [token for token in re.findall(r"[A-Za-z0-9]+", address_text.upper()) if token]
    if not tokens:
        return ""
    for index, token in enumerate(tokens):
        if token.isdigit() and index > 0:
            return tokens[index - 1]
    return ""


def _normalize_unit_token(token: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", token).upper()


def _subject_where_clauses(subject_address: str, address_field: str) -> list[str]:
    if not address_field:
        return []

    subject_unit = _extract_subject_unit(subject_address)
    stripped = _strip_unit_tokens(subject_address).split(",")[0].upper()
    tokens = re.findall(r"[A-Z0-9]+", stripped)
    if not tokens:
        return []

    number_tokens = [token for token in tokens if token.isdigit()]
    text_tokens = [token for token in tokens if not token.isdigit()]

    stop_words = {
        "AB",
        "CALGARY",
        "SW",
        "SE",
        "NW",
        "NE",
        "ST",
        "STREET",
        "AV",
        "AVENUE",
        "RD",
        "ROAD",
        "DR",
        "DRIVE",
        "CT",
        "COURT",
        "BV",
        "BOULEVARD",
    }
    significant = [token for token in text_tokens if token not in stop_words]
    if not number_tokens or not significant:
        return []

    civic = number_tokens[0]
    primary = significant[0]
    clauses: list[str] = []
    if subject_unit:
        clauses.append(
            (
                f"upper({address_field}) like '%{subject_unit}%' and "
                f"upper({address_field}) like '%{civic}%' and "
                f"upper({address_field}) like '%{primary}%'"
            )
        )
    clauses.append(f"upper({address_field}) like '%{civic}%' and upper({address_field}) like '%{primary}%'")
    if len(significant) >= 2:
        secondary = significant[1]
        clauses.append(
            (
                f"upper({address_field}) like '%{civic}%' and "
                f"upper({address_field}) like '%{primary}%' and "
                f"upper({address_field}) like '%{secondary}%'"
            )
        )
    return clauses


def _pick_field(
    field_names: list[str],
    exact_candidates: list[str],
    contains_candidates: list[str],
) -> str:
    lowered_to_original = {name.lower(): name for name in field_names}
    for candidate in exact_candidates:
        match = lowered_to_original.get(candidate.lower())
        if match:
            return match
    for field_name in field_names:
        lowered = field_name.lower()
        if any(token in lowered for token in contains_candidates):
            return field_name
    return ""


def _http_get_json(
    url: str,
    params: dict[str, Any],
    app_token: str,
    timeout_seconds: int,
) -> tuple[Any, str]:
    query = urlencode(params, doseq=True)
    full_url = f"{url}?{query}" if query else url
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    request = Request(full_url, headers=headers)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} while fetching {full_url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error while fetching {full_url}: {exc}") from exc
    try:
        return json.loads(payload), full_url
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from {full_url}") from exc


def _fetch_rows_paginated(
    dataset_id: str,
    base_params: dict[str, Any],
    max_rows: int,
    page_size: int,
    app_token: str,
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    all_rows: list[dict[str, Any]] = []
    queried_urls: list[str] = []
    offset = 0
    resource_url = RESOURCE_URL_TEMPLATE.format(dataset_id=dataset_id)

    while offset < max_rows:
        batch_limit = min(page_size, max_rows - offset)
        params = dict(base_params)
        params["$limit"] = batch_limit
        params["$offset"] = offset
        payload, full_url = _http_get_json(resource_url, params, app_token, timeout_seconds)
        queried_urls.append(full_url)
        if not isinstance(payload, list):
            raise RuntimeError(f"Expected list payload from {full_url}")
        if not payload:
            break
        all_rows.extend(payload)
        if len(payload) < batch_limit:
            break
        offset += len(payload)
    return all_rows, queried_urls


def _load_field_names(dataset_id: str, app_token: str, timeout_seconds: int) -> list[str]:
    views_url = VIEWS_URL_TEMPLATE.format(dataset_id=dataset_id)
    payload, _ = _http_get_json(views_url, {}, app_token, timeout_seconds)
    columns = payload.get("columns", [])
    field_names: list[str] = []
    for column in columns:
        field_name = column.get("fieldName")
        if field_name:
            field_names.append(field_name)
    if not field_names:
        raise RuntimeError(f"No columns discovered for dataset {dataset_id}")
    return field_names


def _detect_fields(field_names: list[str]) -> dict[str, str]:
    return {
        "address": _pick_field(
            field_names,
            ["address", "property_address", "full_address"],
            ["address", "street"],
        ),
        "unit": _pick_field(
            field_names,
            ["unit", "unit_number", "suite_number", "suite_no", "condo_unit"],
            ["unit", "suite", "apt", "condo"],
        ),
        "community": _pick_field(
            field_names,
            ["community_name", "community", "comm_name", "community_code"],
            ["community", "comm", "neigh", "district"],
        ),
        "assessed_value": _pick_field(
            field_names,
            ["assessed_value", "assessment_value", "total_assessed_value"],
            ["assess", "market_value", "taxable_value"],
        ),
        "assessment_year": _pick_field(
            field_names,
            ["assessment_year", "tax_year", "roll_year"],
            ["assessment_year", "tax_year", "roll_year"],
        ),
        "property_type": _pick_field(
            field_names,
            ["property_type", "assessment_class", "land_use_class"],
            ["property_type", "class", "land_use"],
        ),
        "sqft": _pick_field(
            field_names,
            ["total_sqft", "building_area", "assessed_building_area", "net_area", "living_area"],
            ["sqft", "square", "area", "living"],
        ),
        "roll_number": _pick_field(
            field_names,
            ["roll_number", "roll_num", "account_number"],
            ["roll", "account"],
        ),
    }


def _score_subject_candidate(
    row: dict[str, Any],
    fields: dict[str, str],
    subject_address: str,
    subject_unit: str,
) -> int:
    address_field = fields.get("address", "")
    unit_field = fields.get("unit", "")
    address_text = str(row.get(address_field, "")).lower()
    unit_text = str(row.get(unit_field, "")).lower()
    all_text = json.dumps(row, sort_keys=True).lower()
    score = 0
    subject_lower = subject_address.lower()
    if subject_lower in all_text:
        score += 8
    for token in _strip_unit_tokens(subject_address).lower().split():
        if token and token in address_text:
            score += 1
    unit_match = re.search(r"#\s*([a-z0-9\-]+)", subject_address.lower())
    if unit_match and unit_match.group(1) in unit_text:
        score += 4
    if subject_unit:
        normalized_subject_unit = _normalize_unit_token(subject_unit)
        normalized_unit_field = _normalize_unit_token(unit_text)
        normalized_address_suite = _normalize_unit_token(_extract_suite_token_from_address(address_text))
        candidate_units = {item for item in [normalized_unit_field, normalized_address_suite] if item}
        if normalized_subject_unit in candidate_units:
            score += 12
        elif candidate_units:
            score -= 6
    return score


def _select_subject_row(
    rows: list[dict[str, Any]],
    fields: dict[str, str],
    subject_address: str,
    subject_unit: str,
) -> dict[str, Any]:
    if not rows:
        raise RuntimeError("No rows returned for subject search.")
    scored = sorted(
        rows,
        key=lambda row: _score_subject_candidate(row, fields, subject_address, subject_unit),
        reverse=True,
    )
    return scored[0]


def _is_virtual_suite_record(address_text: str) -> bool:
    suite = _extract_suite_token_from_address(address_text)
    if not suite:
        return False
    return _normalize_unit_token(suite).endswith("V")


def _soql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _same_row(a: dict[str, Any], b: dict[str, Any], fields: dict[str, str]) -> bool:
    roll_field = fields.get("roll_number", "")
    if roll_field:
        left_roll = str(a.get(roll_field, "")).strip()
        right_roll = str(b.get(roll_field, "")).strip()
        if left_roll and right_roll and left_roll == right_roll:
            return True
    address_field = fields.get("address", "")
    unit_field = fields.get("unit", "")
    left_address = _normalize_space(str(a.get(address_field, "")).lower())
    right_address = _normalize_space(str(b.get(address_field, "")).lower())
    left_unit = _normalize_space(str(a.get(unit_field, "")).lower())
    right_unit = _normalize_space(str(b.get(unit_field, "")).lower())
    if not left_address and not right_address:
        return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    return bool(left_address and left_address == right_address and left_unit == right_unit)


def _row_identity_key(row: dict[str, Any], fields: dict[str, str]) -> str:
    roll_field = fields.get("roll_number", "")
    roll_value = str(row.get(roll_field, "")).strip() if roll_field else ""
    if roll_value:
        return f"roll:{roll_value}"
    address_field = fields.get("address", "")
    unit_field = fields.get("unit", "")
    address_value = _normalize_space(str(row.get(address_field, "")).lower())
    unit_value = _normalize_space(str(row.get(unit_field, "")).lower())
    if address_value:
        return f"addr:{address_value}|unit:{unit_value}"
    return f"raw:{json.dumps(row, sort_keys=True)}"


def _dedupe_rows(rows: list[dict[str, Any]], fields: dict[str, str]) -> list[dict[str, Any]]:
    unique_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in rows:
        key = _row_identity_key(row, fields)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_rows.append(row)
    return unique_rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _append_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    if file_exists:
        with path.open("r", newline="", encoding="utf-8") as read_handle:
            reader = csv.reader(read_handle)
            existing_header = next(reader, [])
        if existing_header and existing_header != fieldnames:
            raise RuntimeError(f"Header mismatch in {path}; expected {fieldnames} but found {existing_header}")
    with path.open("a", newline="", encoding="utf-8") as write_handle:
        writer = csv.DictWriter(write_handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch open-data assessment comparables from City of Calgary Socrata "
            "and write normalized CSV + provenance."
        )
    )
    parser.add_argument("--subject-address", required=True, help="Full address string for subject search.")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID, help="Socrata dataset id.")
    parser.add_argument("--app-token", default="", help="Socrata app token (or set SOCRATA_APP_TOKEN).")
    parser.add_argument("--search-limit", type=int, default=200, help="Max rows for subject $q search.")
    parser.add_argument("--community-limit", type=int, default=3000, help="Max rows for community query.")
    parser.add_argument("--max-comps", type=int, default=25, help="Max comparable rows to keep.")
    parser.add_argument("--page-size", type=int, default=1000, help="Socrata page size for pagination.")
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV), help="Output CSV for normalized comps.")
    parser.add_argument(
        "--evidence-dir",
        default=str(DEFAULT_EVIDENCE_DIR),
        help="Directory for raw JSON evidence artifacts.",
    )
    parser.add_argument(
        "--source-registry-path",
        default=str(DEFAULT_SOURCE_REGISTRY),
        help="Source registry CSV path.",
    )
    parser.add_argument(
        "--comps-raw-path",
        default=str(DEFAULT_COMPS_RAW),
        help="Raw comps CSV path for optional append.",
    )
    parser.add_argument(
        "--append-comps-raw",
        action="store_true",
        help="Append generated rows into data/comps_raw.csv as assessment_proxy.",
    )
    parser.add_argument(
        "--no-source-registry",
        action="store_true",
        help="Skip appending source provenance rows.",
    )
    parser.add_argument(
        "--captured-by",
        default=getpass.getuser(),
        help="Value for captured_by field in provenance rows.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print detected queries/fields only; do not call remote APIs.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print query attempts and detected fields during execution.",
    )
    parser.add_argument(
        "--subject-search-mode",
        choices=("where_only", "where_then_q", "q_then_where"),
        default="where_only",
        help=(
            "Subject row lookup strategy. "
            "where_only (default) avoids Socrata $q and uses $where clauses only."
        ),
    )
    parser.add_argument(
        "--exclude-property-types",
        default="",
        help="Comma-delimited property_type values to exclude.",
    )
    parser.add_argument(
        "--include-property-types",
        default="",
        help="Comma-delimited allow-list for property_type values.",
    )
    parser.add_argument(
        "--min-assessed-value",
        type=float,
        default=50000.0,
        help="Minimum assessed value to keep as comparable (default: 50000).",
    )
    parser.add_argument(
        "--include-virtual-suites",
        action="store_true",
        help="Include records whose suite token ends with 'V'.",
    )
    parser.add_argument(
        "--match-subject-property-type",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep comps with same property_type as subject (default: enabled).",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    dataset_id = args.dataset_id
    subject_address = _normalize_space(args.subject_address)
    subject_unit = _extract_subject_unit(subject_address)
    app_token = args.app_token or os.environ.get("SOCRATA_APP_TOKEN", "")
    exclude_property_types = _parse_csv_set(args.exclude_property_types)
    include_property_types = _parse_csv_set(args.include_property_types)
    include_virtual_suites = bool(args.include_virtual_suites)

    output_csv_path = Path(args.output_csv)
    evidence_dir = Path(args.evidence_dir)
    source_registry_path = Path(args.source_registry_path)
    comps_raw_path = Path(args.comps_raw_path)
    run_timestamp = dt.datetime.now(dt.timezone.utc)
    run_id = run_timestamp.strftime("%Y%m%dT%H%M%SZ")

    try:
        if args.dry_run:
            query_preview = _build_subject_queries(subject_address)
            where_preview = _subject_where_clauses(subject_address, "address")
            print("Dry run enabled. Planned subject lookup attempts:")
            if args.subject_search_mode == "where_only":
                for item in where_preview:
                    print(f"- $where: {item}")
                if not where_preview:
                    for item in query_preview:
                        print(f"- $q: {item}")
            elif args.subject_search_mode == "where_then_q":
                for item in where_preview:
                    print(f"- $where: {item}")
                for item in query_preview:
                    print(f"- $q: {item}")
            else:
                for item in query_preview:
                    print(f"- $q: {item}")
                for item in where_preview:
                    print(f"- $where: {item}")
            return 0

        field_names = _load_field_names(dataset_id, app_token, args.timeout_seconds)
        fields = _detect_fields(field_names)
        if not fields.get("assessed_value"):
            raise RuntimeError("Could not identify assessed value field in dataset schema.")
        if args.debug:
            print(f"DEBUG: detected fields -> {fields}")
            print(f"DEBUG: subject_unit -> {subject_unit or '(none)'}")

        subject_rows: list[dict[str, Any]] = []
        subject_query_urls: list[str] = []
        subject_queries = _build_subject_queries(subject_address)
        where_clauses = _subject_where_clauses(subject_address, fields.get("address", ""))
        lookup_attempts: list[tuple[str, str]] = []

        if args.subject_search_mode == "where_only":
            lookup_attempts.extend([("$where", where_clause) for where_clause in where_clauses])
            if not lookup_attempts:
                # Fallback when tokenized address cannot build a WHERE clause.
                lookup_attempts.extend([("$q", query) for query in subject_queries])
        elif args.subject_search_mode == "where_then_q":
            lookup_attempts.extend([("$where", where_clause) for where_clause in where_clauses])
            lookup_attempts.extend([("$q", query) for query in subject_queries])
        else:  # q_then_where
            lookup_attempts.extend([("$q", query) for query in subject_queries])
            lookup_attempts.extend([("$where", where_clause) for where_clause in where_clauses])

        for operator, value in lookup_attempts:
            rows, urls = _fetch_rows_paginated(
                dataset_id=dataset_id,
                base_params={operator: value},
                max_rows=args.search_limit,
                page_size=args.page_size,
                app_token=app_token,
                timeout_seconds=args.timeout_seconds,
            )
            if args.debug:
                print(f"DEBUG: subject {operator}='{value}' -> {len(rows)} row(s)")
            subject_rows.extend(rows)
            subject_query_urls.extend(urls)
            if rows:
                break

        if not subject_rows:
            debug_lines = [
                "No rows returned for subject lookup.",
                f"Subject address: {subject_address}",
                f"Dataset id: {dataset_id}",
                f"Address field detected: {fields.get('address', '(none)')}",
                f"Subject search mode: {args.subject_search_mode}",
                f"Tried $q queries: {subject_queries}",
            ]
            if where_clauses:
                debug_lines.append(f"Tried $where clauses: {where_clauses}")
            debug_lines.append(
                "Try running with --debug and/or pass a simpler address like '3000 SOMERVALE CT SW'."
            )
            raise RuntimeError(" ".join(debug_lines))

        property_type_field = fields.get("property_type", "")
        address_field_for_matching = fields.get("address", "")

        subject_rows = _dedupe_rows(subject_rows, fields)
        subject_filtered_rows: list[dict[str, Any]] = []
        subject_drop_reasons: Counter[str] = Counter()
        for row in subject_rows:
            property_type = (
                str(row.get(property_type_field, "")).strip().upper()
                if property_type_field
                else ""
            )
            assessed_value = _to_float(row.get(fields["assessed_value"]))
            address_text = str(row.get(address_field_for_matching, "")).strip()

            if include_property_types and property_type not in include_property_types:
                subject_drop_reasons["include_property_types"] += 1
                continue
            if exclude_property_types and property_type in exclude_property_types:
                subject_drop_reasons["exclude_property_types"] += 1
                continue
            if assessed_value is not None and assessed_value < args.min_assessed_value:
                subject_drop_reasons["min_assessed_value"] += 1
                continue
            if (not include_virtual_suites) and _is_virtual_suite_record(address_text):
                subject_drop_reasons["virtual_suite"] += 1
                continue
            subject_filtered_rows.append(row)

        subject_rows_for_selection = subject_filtered_rows or subject_rows
        if args.debug and subject_drop_reasons:
            print(f"DEBUG: subject filter drop reasons -> {dict(subject_drop_reasons)}")

        subject_row = _select_subject_row(
            subject_rows_for_selection,
            fields,
            subject_address,
            subject_unit,
        )

        community_field = fields.get("community", "")
        community_value = str(subject_row.get(community_field, "")).strip() if community_field else ""
        subject_property_type = str(subject_row.get(property_type_field, "")).strip()

        subject_property_type_upper = subject_property_type.upper()
        effective_exclude_property_types = set(exclude_property_types)
        if (
            not include_property_types
            and subject_property_type_upper
            and subject_property_type_upper in effective_exclude_property_types
        ):
            effective_exclude_property_types.discard(subject_property_type_upper)
            if args.debug:
                print(
                    "DEBUG: removed subject property_type from exclude list -> "
                    f"{subject_property_type_upper}"
                )

        candidate_rows: list[dict[str, Any]] = []
        candidate_query_urls: list[str] = []
        candidate_scope = "address"
        candidate_where_clauses = _subject_where_clauses(subject_address, address_field_for_matching)

        for where_clause in candidate_where_clauses:
            rows, urls = _fetch_rows_paginated(
                dataset_id=dataset_id,
                base_params={"$where": where_clause},
                max_rows=args.community_limit,
                page_size=args.page_size,
                app_token=app_token,
                timeout_seconds=args.timeout_seconds,
            )
            if args.debug:
                print(f"DEBUG: candidate $where='{where_clause}' -> {len(rows)} row(s)")
            candidate_rows.extend(rows)
            candidate_query_urls.extend(urls)
        candidate_rows = _dedupe_rows(candidate_rows, fields)

        if not candidate_rows:
            if community_field and community_value:
                candidate_scope = "community"
                where_clause = f"upper({community_field}) = {_soql_quote(community_value.upper())}"
                rows, urls = _fetch_rows_paginated(
                    dataset_id=dataset_id,
                    base_params={"$where": where_clause},
                    max_rows=args.community_limit,
                    page_size=args.page_size,
                    app_token=app_token,
                    timeout_seconds=args.timeout_seconds,
                )
                if args.debug:
                    print(f"DEBUG: candidate community $where='{where_clause}' -> {len(rows)} row(s)")
                candidate_rows = _dedupe_rows(rows, fields)
                candidate_query_urls = urls
            else:
                candidate_scope = "subject_rows"
                candidate_rows = subject_rows[:]
                candidate_query_urls = subject_query_urls[:]

        candidate_rows = [row for row in candidate_rows if not _same_row(row, subject_row, fields)]

        addressed_field = fields.get("address", "")
        assessed_field = fields["assessed_value"]
        filtered_rows: list[dict[str, Any]] = []
        drop_reasons: Counter[str] = Counter()
        match_subject_property_type = bool(args.match_subject_property_type and subject_property_type_upper)
        for row in candidate_rows:
            property_type = (
                str(row.get(property_type_field, "")).strip().upper()
                if property_type_field
                else ""
            )
            assessed_value = _to_float(row.get(assessed_field))
            address_text = str(row.get(addressed_field, "")).strip()

            if (
                match_subject_property_type
                and property_type
                and property_type != subject_property_type_upper
            ):
                drop_reasons["match_subject_property_type"] += 1
                continue
            if include_property_types and property_type not in include_property_types:
                drop_reasons["include_property_types"] += 1
                continue
            if effective_exclude_property_types and property_type in effective_exclude_property_types:
                drop_reasons["exclude_property_types"] += 1
                continue
            if assessed_value is not None and assessed_value < args.min_assessed_value:
                drop_reasons["min_assessed_value"] += 1
                continue
            if (not include_virtual_suites) and _is_virtual_suite_record(address_text):
                drop_reasons["virtual_suite"] += 1
                continue
            filtered_rows.append(row)
        candidate_rows = filtered_rows

        if args.debug:
            if drop_reasons:
                print(f"DEBUG: filter drop reasons -> {dict(drop_reasons)}")
            print(f"DEBUG: candidate scope -> {candidate_scope}")

        subject_assessed_value = _to_float(subject_row.get(assessed_field))
        if subject_assessed_value:
            candidate_rows.sort(
                key=lambda row: abs(
                    (_to_float(row.get(assessed_field)) or subject_assessed_value) - subject_assessed_value
                )
            )
        candidate_rows = candidate_rows[: args.max_comps]

        if not candidate_rows:
            property_type_hist = Counter(
                str(row.get(property_type_field, "")).strip().upper() if property_type_field else "(none)"
                for row in subject_rows
            )
            raise RuntimeError(
                "No comparable rows found after filtering. "
                f"subject_unit={subject_unit or '(none)'} "
                f"subject_property_type={subject_property_type or '(none)'} "
                f"match_subject_property_type={match_subject_property_type} "
                f"effective_exclude_property_types={sorted(effective_exclude_property_types)} "
                f"property_type_hist={dict(property_type_hist)} "
                "Try --include-virtual-suites and/or --exclude-property-types \"\" and/or a lower --min-assessed-value."
            )

        normalized_rows: list[dict[str, Any]] = []
        source_registry_rows: list[dict[str, Any]] = []
        comps_raw_rows: list[dict[str, Any]] = []

        address_field = fields.get("address", "")
        unit_field = fields.get("unit", "")
        year_field = fields.get("assessment_year", "")
        sqft_field = fields.get("sqft", "")
        roll_field = fields.get("roll_number", "")

        evidence_dataset_dir = evidence_dir / dataset_id
        evidence_dataset_dir.mkdir(parents=True, exist_ok=True)
        evidence_path = evidence_dataset_dir / f"{run_id}_fetch.json"

        query_urls = sorted(set(subject_query_urls + candidate_query_urls))
        primary_source_url = (
            candidate_query_urls[0]
            if candidate_query_urls
            else (subject_query_urls[0] if subject_query_urls else "")
        )
        evidence_payload = {
            "dataset_id": dataset_id,
            "run_id": run_id,
            "subject_address_input": subject_address,
            "detected_fields": fields,
            "query_urls": query_urls,
            "candidate_scope": candidate_scope,
            "match_subject_property_type": match_subject_property_type,
            "effective_exclude_property_types": sorted(effective_exclude_property_types),
            "subject_row": subject_row,
            "candidate_count_raw": len(candidate_rows),
            "captured_at": run_timestamp.isoformat(),
        }
        evidence_path.write_text(json.dumps(evidence_payload, indent=2), encoding="utf-8")
        evidence_hash = _hash_file(evidence_path)

        for index, row in enumerate(candidate_rows, start=1):
            comp_id = f"C-OC-{run_id}-{index:03d}"
            source_id = f"S-OC-{run_id}-{index:03d}"
            assessed_value = _to_float(row.get(assessed_field))
            value_delta = None
            value_delta_pct = None
            if assessed_value is not None and subject_assessed_value:
                value_delta = assessed_value - subject_assessed_value
                value_delta_pct = value_delta / subject_assessed_value

            normalized_row = {
                "comp_id": comp_id,
                "source_id": source_id,
                "address": str(row.get(address_field, "")).strip(),
                "unit": str(row.get(unit_field, "")).strip(),
                "community": str(row.get(community_field, "")).strip() if community_field else "",
                "assessed_value": f"{assessed_value:.0f}" if assessed_value is not None else "",
                "assessment_year": str(row.get(year_field, "")).strip() if year_field else "",
                "property_type": str(row.get(property_type_field, "")).strip()
                if property_type_field
                else "",
                "sqft": str(row.get(sqft_field, "")).strip() if sqft_field else "",
                "roll_number": str(row.get(roll_field, "")).strip() if roll_field else "",
                "value_delta": f"{value_delta:.0f}" if value_delta is not None else "",
                "value_delta_pct": f"{value_delta_pct:.4f}" if value_delta_pct is not None else "",
                "dataset_id": dataset_id,
                "source_url": primary_source_url,
            }
            normalized_rows.append(normalized_row)

            source_registry_rows.append(
                {
                    "source_id": source_id,
                    "comp_id": comp_id,
                    "source_type": "open_calgary_api",
                    "mls_number": "",
                    "url": primary_source_url,
                    "publisher": "City of Calgary Open Data",
                    "captured_at": run_timestamp.isoformat(),
                    "captured_by": args.captured_by,
                    "file_path": str(evidence_path),
                    "file_sha256": evidence_hash,
                    "claims_supported": "address;community;assessed_value;property_type;sqft",
                    "notes": f"Dataset {dataset_id}; assessment proxy data, not MLS sold prices.",
                }
            )

            if args.append_comps_raw:
                comps_raw_rows.append(
                    {
                        "comp_id": comp_id,
                        "address": normalized_row["address"],
                        "unit": normalized_row["unit"],
                        "status": "assessment_proxy",
                        "list_price": "",
                        "sold_price": "",
                        "sale_date": "",
                        "sqft": normalized_row["sqft"],
                        "bedrooms": "",
                        "bathrooms": "",
                        "parking": "",
                        "dom": "",
                        "condition": "",
                        "origin_notes": (
                            f"assessed_value={normalized_row['assessed_value']}; "
                            f"value_delta_pct={normalized_row['value_delta_pct']}; "
                            f"source_id={source_id}; dataset={dataset_id}; "
                            "open-data assessment proxy, not closed sale."
                        ),
                    }
                )

        _write_csv(output_csv_path, ASSESSMENT_COMPS_FIELDNAMES, normalized_rows)
        if not args.no_source_registry:
            _append_csv(source_registry_path, SOURCE_REGISTRY_FIELDNAMES, source_registry_rows)
        if args.append_comps_raw:
            _append_csv(comps_raw_path, COMPS_RAW_FIELDNAMES, comps_raw_rows)

        print(f"Subject address: {subject_address}")
        print(f"Subject unit token: {subject_unit or '(none)'}")
        print(f"Dataset: {dataset_id}")
        print(f"Detected fields: {fields}")
        print(f"Candidate scope: {candidate_scope}")
        print(f"Wrote {len(normalized_rows)} rows to {output_csv_path}")
        print(f"Wrote evidence artifact: {evidence_path}")
        if not args.no_source_registry:
            print(f"Appended {len(source_registry_rows)} provenance rows to {source_registry_path}")
        if args.append_comps_raw:
            print(f"Appended {len(comps_raw_rows)} rows to {comps_raw_path}")
        print("Done.")
        return 0

    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
