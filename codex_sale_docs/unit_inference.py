import argparse
import csv
import datetime as dt
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from codex_sale_docs.open_calgary import (
    DEFAULT_DATASET_ID,
    DEFAULT_EVIDENCE_DIR,
    _build_subject_queries,
    _dedupe_rows,
    _detect_fields,
    _extract_subject_unit,
    _fetch_rows_paginated,
    _is_virtual_suite_record,
    _load_field_names,
    _normalize_space,
    _normalize_unit_token,
    _parse_csv_set,
    _row_identity_key,
    _same_row,
    _select_subject_row,
    _strip_unit_tokens,
    _subject_where_clauses,
    _to_float,
)


DEFAULT_OUTPUT_CSV = Path("data/open_calgary_inferred_unit_comps.csv")
DEFAULT_RELATED_CIVICS = "1000,2000,3000,5500,7000"
DEFAULT_EXTRA_BUILDINGS = ["720 Stoney Trail SW, Calgary AB T2Y 4M1"]
DEFAULT_SAME_PLAN_CSV = Path("data/open_calgary_same_floorplan_units.csv")
DEFAULT_SIMILAR_PLAN_CSV = Path("data/open_calgary_similar_floorplan_units.csv")
DEFAULT_PLAN_GROUPS_CSV = Path("data/open_calgary_floorplan_groups.csv")

INFERRED_FIELDNAMES = [
    "rank",
    "comp_id",
    "source_id",
    "target_building",
    "address",
    "unit",
    "subject_unit",
    "unit_match",
    "unit_score",
    "floor_plan_relation",
    "relation_reason",
    "plan_signature",
    "metric_similarity_pct",
    "metric_match_count",
    "metric_total_count",
    "metric_fields_used",
    "metric_fields_matched",
    "community",
    "assessed_value",
    "re_assessed_value",
    "nr_assessed_value",
    "fl_assessed_value",
    "assessment_year",
    "year_of_construction",
    "property_type",
    "assessment_class",
    "assessment_class_description",
    "land_use_designation",
    "sub_property_use",
    "land_size_sm",
    "sqft",
    "roll_number",
    "value_delta",
    "value_delta_pct",
    "dataset_id",
    "source_url",
]

PLAN_GROUP_FIELDNAMES = [
    "group_rank",
    "plan_signature",
    "row_count",
    "same_floor_plan_count",
    "similar_floor_plan_count",
    "other_count",
    "buildings",
    "units",
    "assessed_value_min",
    "assessed_value_max",
    "value_delta_pct_min",
    "value_delta_pct_max",
]

PRIORITY_TEXT_METRICS = [
    "property_type",
    "assessment_class",
    "assessment_class_description",
    "land_use_designation",
    "sub_property_use",
    "comm_code",
    "comm_name",
]

PRIORITY_NUMERIC_METRICS = [
    "assessed_value",
    "re_assessed_value",
    "nr_assessed_value",
    "fl_assessed_value",
    "land_size_sm",
    "land_size_sf",
    "land_size_ac",
    "year_of_construction",
]

METRIC_IGNORE_FIELDS = {
    "address",
    "roll_number",
    "unique_key",
    "cpid",
    "mod_date",
    "multipolygon",
    "the_geom",
    "shape",
    "latitude",
    "longitude",
    "x",
    "y",
}


def _parse_csv_list(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _unique_preserve(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = _normalize_space(value).upper()
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(_normalize_space(value))
    return unique


def _subject_building_address(subject_address: str) -> str:
    local = _strip_unit_tokens(subject_address).split(",")[0]
    return _normalize_space(local)


def _subject_street_portion(subject_building_address: str) -> str:
    match = re.match(r"^\s*\d{3,6}\s+(.+)$", subject_building_address)
    if match:
        return _normalize_space(match.group(1))
    return subject_building_address


def _street_name_components(street_portion: str) -> tuple[str, list[str]]:
    tokens = [token for token in re.findall(r"[A-Z0-9]+", street_portion.upper()) if not token.isdigit()]
    if not tokens:
        return "", []
    direction_tokens = {"N", "S", "E", "W", "NE", "NW", "SE", "SW", "AB", "CALGARY"}
    tokens = [token for token in tokens if token not in direction_tokens]
    if not tokens:
        return "", []

    street_name = tokens[0]
    street_type = tokens[1] if len(tokens) >= 2 else ""
    street_type_aliases = {
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
    aliases = street_type_aliases.get(street_type, [street_type] if street_type else [])
    # Preserve order while deduping aliases.
    seen: set[str] = set()
    deduped_aliases: list[str] = []
    for alias in aliases:
        if alias and alias not in seen:
            seen.add(alias)
            deduped_aliases.append(alias)
    return street_name, deduped_aliases


def _street_where_clauses(street_portion: str, address_field: str) -> list[str]:
    if not street_portion or not address_field:
        return []
    street_name, street_type_aliases = _street_name_components(street_portion)
    if not street_name:
        return []

    clauses: list[str] = []
    if street_type_aliases:
        for alias in street_type_aliases:
            clauses.append(f"upper({address_field}) like '%{street_name} {alias}%'")
        return clauses
    return [f"upper({address_field}) like '%{street_name}%'"]


def _extract_civic_from_address(address_text: str) -> str:
    match = re.match(r"^\s*(?:[A-Za-z0-9\-]+\s+)?(\d{3,6})\b", address_text.strip())
    if not match:
        return ""
    return match.group(1)


def _sort_civic_tokens(values: set[str]) -> list[str]:
    def _sort_key(token: str) -> tuple[int, str]:
        if token.isdigit():
            return (0, f"{int(token):06d}")
        return (1, token)

    return sorted(values, key=_sort_key)


def _discover_street_civics(
    *,
    dataset_id: str,
    street_portion: str,
    address_field: str,
    app_token: str,
    timeout_seconds: int,
    page_size: int,
    max_rows: int,
    debug: bool,
) -> tuple[list[str], list[str]]:
    clauses = _street_where_clauses(street_portion, address_field)
    if not clauses:
        return [], []

    civics: set[str] = set()
    query_urls: list[str] = []
    street_name, street_type_aliases = _street_name_components(street_portion)
    for clause in clauses:
        rows, urls = _fetch_rows_paginated(
            dataset_id=dataset_id,
            base_params={"$where": clause},
            max_rows=max_rows,
            page_size=page_size,
            app_token=app_token,
            timeout_seconds=timeout_seconds,
        )
        query_urls.extend(urls)
        if debug:
            print(f"DEBUG: street discovery $where='{clause}' -> {len(rows)} row(s)")
        for row in rows:
            address_text = str(row.get(address_field, "")).strip().upper()
            if not address_text:
                continue
            # Guard against near-street noise by requiring matching street name/type in the parsed address tail.
            tail_match = re.match(r"^\s*(?:[A-Z0-9\-]+\s+)?\d{3,6}\s+(.+)$", address_text)
            if not tail_match:
                continue
            tail_tokens = set(re.findall(r"[A-Z0-9]+", tail_match.group(1)))
            if street_name and street_name not in tail_tokens:
                continue
            if street_type_aliases and not any(alias in tail_tokens for alias in street_type_aliases):
                continue
            civic = _extract_civic_from_address(address_text)
            # Ignore short civic numbers that cause broad partial matches in LIKE queries.
            if civic and len(civic) >= 4:
                civics.add(civic)
    return _sort_civic_tokens(civics), query_urls


def _extract_row_unit(row: dict[str, Any], fields: dict[str, str]) -> str:
    unit_field = fields.get("unit", "")
    address_field = fields.get("address", "")
    if unit_field:
        unit_value = _normalize_unit_token(str(row.get(unit_field, "")).strip())
        if unit_value:
            return unit_value
    address_text = str(row.get(address_field, "")).strip()
    match = re.match(r"^\s*([A-Za-z0-9\-]+)\s+\d{3,6}\b", address_text)
    if match:
        return _normalize_unit_token(match.group(1))
    return ""


def _unit_parts(unit_token: str) -> tuple[int | None, str]:
    digits = re.sub(r"[^0-9]", "", unit_token)
    if not digits:
        return None, ""
    if len(digits) >= 3:
        floor_text = digits[:-2]
        floor = int(floor_text) if floor_text else None
        return floor, digits[-2:]
    if len(digits) == 2:
        return None, digits
    return int(digits), digits


def _unit_stack(unit_token: str) -> str:
    _, stack = _unit_parts(unit_token)
    return stack


def _normalize_metric_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        return ""
    return _normalize_space(str(value)).upper()


def _infer_unit_similarity(subject_unit: str, candidate_unit: str) -> tuple[str, int]:
    subject = _normalize_unit_token(subject_unit)
    candidate = _normalize_unit_token(candidate_unit)
    if not subject or not candidate:
        return "unknown", 0
    if subject == candidate:
        return "same_unit", 200

    score = 0
    labels: list[str] = []
    subject_floor, subject_stack = _unit_parts(subject)
    candidate_floor, candidate_stack = _unit_parts(candidate)

    if subject_stack and candidate_stack and subject_stack == candidate_stack:
        score += 100
        labels.append("same_stack")

    if subject_floor is not None and candidate_floor is not None:
        floor_distance = abs(candidate_floor - subject_floor)
        if floor_distance == 0:
            score += 30
            labels.append("same_floor")
        elif floor_distance == 1:
            score += 20
            labels.append("adjacent_floor")
        elif floor_distance <= 3:
            score += 10
            labels.append("near_floor")

    subject_digits = re.sub(r"[^0-9]", "", subject)
    candidate_digits = re.sub(r"[^0-9]", "", candidate)
    if subject_digits and candidate_digits and len(subject_digits) == len(candidate_digits):
        gap = abs(int(candidate_digits) - int(subject_digits))
        score += max(0, 20 - min(gap, 20))

    if not labels:
        labels.append("layout_unknown")
    return "+".join(labels), score


def _iter_metric_fields(subject_row: dict[str, Any], candidate_row: dict[str, Any]) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()

    for field_name in PRIORITY_TEXT_METRICS + PRIORITY_NUMERIC_METRICS:
        if field_name in seen:
            continue
        if field_name in subject_row or field_name in candidate_row:
            seen.add(field_name)
            fields.append(field_name)

    shared_keys = sorted(set(subject_row) & set(candidate_row))
    for field_name in shared_keys:
        if field_name in seen or field_name in METRIC_IGNORE_FIELDS:
            continue
        seen.add(field_name)
        fields.append(field_name)

    return fields


def _compute_metric_similarity(subject_row: dict[str, Any], candidate_row: dict[str, Any]) -> dict[str, Any]:
    compared_fields: list[str] = []
    matched_fields: list[str] = []
    score_total = 0.0
    total_count = 0
    match_count = 0

    for field_name in _iter_metric_fields(subject_row, candidate_row):
        subject_value = subject_row.get(field_name)
        candidate_value = candidate_row.get(field_name)

        subject_num = _to_float(subject_value)
        candidate_num = _to_float(candidate_value)
        if subject_num is not None and candidate_num is not None:
            compared_fields.append(field_name)
            total_count += 1
            denominator = max(abs(subject_num), abs(candidate_num), 1.0)
            closeness = max(0.0, 1.0 - (abs(subject_num - candidate_num) / denominator))
            score_total += closeness
            if closeness >= 0.90:
                match_count += 1
                matched_fields.append(field_name)
            continue

        subject_text = _normalize_metric_text(subject_value)
        candidate_text = _normalize_metric_text(candidate_value)
        if not subject_text or not candidate_text:
            continue
        compared_fields.append(field_name)
        total_count += 1
        if subject_text == candidate_text:
            score_total += 1.0
            match_count += 1
            matched_fields.append(field_name)

    similarity_pct = (score_total / total_count) * 100 if total_count else 0.0
    return {
        "metric_similarity_pct": similarity_pct,
        "metric_match_count": match_count,
        "metric_total_count": total_count,
        "metric_fields_used": compared_fields,
        "metric_fields_matched": matched_fields,
    }


def _build_plan_signature(row: dict[str, Any], unit_token: str) -> str:
    stack = _unit_stack(unit_token) or "NA"
    sub_property_use = _normalize_metric_text(row.get("sub_property_use")) or "NA"
    land_use = _normalize_metric_text(row.get("land_use_designation")) or "NA"
    assessed_value = _to_float(row.get("assessed_value"))
    assessed_bucket = ""
    if assessed_value is not None:
        assessed_bucket = f"{round(assessed_value / 2500) * 2500:.0f}"
    assessed_bucket = assessed_bucket or "NA"
    return (
        f"stack:{stack}|sub_property_use:{sub_property_use}|"
        f"land_use:{land_use}|assessed_bucket:{assessed_bucket}"
    )


def _classify_floor_plan_relation(
    unit_match: str,
    unit_score: int,
    metric_similarity_pct: float,
    matched_fields: set[str],
) -> tuple[str, str]:
    same_stack = "same_stack" in unit_match
    same_floor = "same_floor" in unit_match
    has_sub_property_match = "sub_property_use" in matched_fields

    if unit_match == "same_unit":
        return "same_floor_plan", "same unit identifier across related building"
    if same_stack and (has_sub_property_match or metric_similarity_pct >= 70):
        return "same_floor_plan", "same stack with strong metric similarity"
    if same_floor and (has_sub_property_match or metric_similarity_pct >= 65):
        return "similar_floor_plan", "same floor with matching plan indicators"
    if unit_score >= 100 and metric_similarity_pct >= 60:
        return "similar_floor_plan", "stack/floor proximity with metric support"
    if metric_similarity_pct >= 75:
        return "similar_floor_plan", "high cross-metric similarity"
    return "other", "insufficient floor-plan similarity evidence"


def _sort_inferred_rows(
    row: dict[str, str],
    prefer_high_end: bool,
) -> tuple[float, float, float, float, float]:
    unit_score = _to_float(row.get("unit_score")) or 0.0
    metric_similarity_pct = _to_float(row.get("metric_similarity_pct")) or 0.0
    delta_pct = _to_float(row.get("value_delta_pct"))
    delta_abs = abs(delta_pct) if delta_pct is not None else 10**9
    assessed_value = _to_float(row.get("assessed_value")) or 0.0
    if prefer_high_end:
        non_negative_tier = 0.0 if delta_pct is not None and delta_pct >= 0 else 1.0
        high_end_bias = -(delta_pct if delta_pct is not None else -10**9)
        return (-unit_score, -metric_similarity_pct, non_negative_tier, high_end_bias, -assessed_value)
    return (-unit_score, -metric_similarity_pct, delta_abs, -assessed_value, 0.0)


def _stack_from_plan_signature(plan_signature: str) -> str:
    if not plan_signature:
        return "NA"
    for token in plan_signature.split("|"):
        if token.startswith("stack:"):
            stack = token.split(":", 1)[1].strip()
            return stack or "NA"
    return "NA"


def _apply_max_per_stack(rows: list[dict[str, str]], max_per_stack: int) -> list[dict[str, str]]:
    if max_per_stack <= 0:
        return rows
    selected: list[dict[str, str]] = []
    stack_counts: Counter[str] = Counter()
    for row in rows:
        stack = _stack_from_plan_signature(row.get("plan_signature", ""))
        if stack_counts[stack] >= max_per_stack:
            continue
        selected.append(row)
        stack_counts[stack] += 1
    return selected


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _write_plan_groups_csv(path: Path, rows: list[dict[str, str]]) -> None:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "rows": 0,
            "same_floor_plan": 0,
            "similar_floor_plan": 0,
            "other": 0,
            "buildings": set(),
            "units": set(),
            "assessed_values": [],
            "delta_pcts": [],
        }
    )
    for row in rows:
        signature = row.get("plan_signature", "") or "UNKNOWN"
        bucket = grouped[signature]
        bucket["rows"] += 1
        relation = row.get("floor_plan_relation", "other")
        bucket[relation] += 1
        if row.get("target_building"):
            bucket["buildings"].add(row["target_building"])
        if row.get("unit"):
            bucket["units"].add(row["unit"])
        assessed = _to_float(row.get("assessed_value"))
        if assessed is not None:
            bucket["assessed_values"].append(assessed)
        delta_pct = _to_float(row.get("value_delta_pct"))
        if delta_pct is not None:
            bucket["delta_pcts"].append(delta_pct)

    summary_rows: list[dict[str, str]] = []
    ordered = sorted(
        grouped.items(),
        key=lambda item: (
            -item[1]["same_floor_plan"],
            -item[1]["similar_floor_plan"],
            -item[1]["rows"],
            item[0],
        ),
    )
    for index, (signature, bucket) in enumerate(ordered, start=1):
        assessed_values = bucket["assessed_values"]
        delta_pcts = bucket["delta_pcts"]
        summary_rows.append(
            {
                "group_rank": str(index),
                "plan_signature": signature,
                "row_count": str(bucket["rows"]),
                "same_floor_plan_count": str(bucket["same_floor_plan"]),
                "similar_floor_plan_count": str(bucket["similar_floor_plan"]),
                "other_count": str(bucket["other"]),
                "buildings": "; ".join(sorted(bucket["buildings"])),
                "units": "; ".join(sorted(bucket["units"])),
                "assessed_value_min": (
                    f"{min(assessed_values):.0f}" if assessed_values else ""
                ),
                "assessed_value_max": (
                    f"{max(assessed_values):.0f}" if assessed_values else ""
                ),
                "value_delta_pct_min": (
                    f"{min(delta_pcts):.4f}" if delta_pcts else ""
                ),
                "value_delta_pct_max": (
                    f"{max(delta_pcts):.4f}" if delta_pcts else ""
                ),
            }
        )

    _write_csv(path, PLAN_GROUP_FIELDNAMES, summary_rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Infer comparable condo units from open Calgary assessment data "
            "across related buildings and score likely floor-plan similarity."
        )
    )
    parser.add_argument("--subject-address", required=True, help="Full subject address.")
    parser.add_argument("--subject-unit", default="", help="Optional unit token override (for example: 209).")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID, help="Socrata dataset id.")
    parser.add_argument("--app-token", default="", help="Socrata app token (or set SOCRATA_APP_TOKEN).")
    parser.add_argument("--search-limit", type=int, default=1500, help="Max rows for subject lookup.")
    parser.add_argument(
        "--max-rows-per-building",
        type=int,
        default=4000,
        help="Max rows fetched for each target building address.",
    )
    parser.add_argument(
        "--max-comps",
        type=int,
        default=0,
        help="Max inferred comps to keep (0 means keep all).",
    )
    parser.add_argument(
        "--max-per-stack",
        type=int,
        default=0,
        help=(
            "Maximum rows per unit stack in final ranked output (0 means no cap). "
            "Useful to diversify results beyond one dominant stack."
        ),
    )
    parser.add_argument("--page-size", type=int, default=1000, help="Socrata page size for pagination.")
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument(
        "--related-civic-numbers",
        default=DEFAULT_RELATED_CIVICS,
        help="Comma-delimited civic numbers on the subject street.",
    )
    parser.add_argument(
        "--all-street-buildings",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Discover civic numbers from all records on the subject street and include "
            "them in target buildings."
        ),
    )
    parser.add_argument(
        "--street-discovery-max-rows",
        type=int,
        default=10000,
        help="Max rows to scan when --all-street-buildings is enabled.",
    )
    parser.add_argument(
        "--building-address",
        action="append",
        default=[],
        help="Explicit additional building address (repeatable).",
    )
    parser.add_argument(
        "--include-default-extra-buildings",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include default extra building addresses (default: enabled).",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Output CSV path for inferred unit comps.",
    )
    parser.add_argument(
        "--same-floor-plan-csv",
        default=str(DEFAULT_SAME_PLAN_CSV),
        help="Output CSV path for rows classified as same_floor_plan.",
    )
    parser.add_argument(
        "--similar-floor-plan-csv",
        default=str(DEFAULT_SIMILAR_PLAN_CSV),
        help="Output CSV path for rows classified as similar_floor_plan.",
    )
    parser.add_argument(
        "--plan-groups-csv",
        default=str(DEFAULT_PLAN_GROUPS_CSV),
        help="Output CSV path for grouped plan signature summary.",
    )
    parser.add_argument(
        "--evidence-dir",
        default=str(DEFAULT_EVIDENCE_DIR / "unit_inference"),
        help="Evidence output directory.",
    )
    parser.add_argument(
        "--exclude-property-types",
        default="",
        help="Comma-delimited property_type values to exclude.",
    )
    parser.add_argument(
        "--include-property-types",
        default="",
        help="Comma-delimited property_type allow-list.",
    )
    parser.add_argument(
        "--min-assessed-value",
        type=float,
        default=100000.0,
        help="Minimum assessed value to keep as comparable.",
    )
    parser.add_argument(
        "--include-virtual-suites",
        action="store_true",
        help="Include records whose suite token ends with V.",
    )
    parser.add_argument(
        "--match-subject-property-type",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require same property_type as subject (default: enabled).",
    )
    parser.add_argument(
        "--prefer-high-end",
        action="store_true",
        help="When sorting, prioritize higher-value non-negative deltas after similarity scores.",
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
    parser.add_argument("--debug", action="store_true", help="Print diagnostic query/filter output.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    subject_address = _normalize_space(args.subject_address)
    subject_unit = _normalize_unit_token(args.subject_unit) or _extract_subject_unit(subject_address)
    include_virtual_suites = bool(args.include_virtual_suites)
    include_property_types = _parse_csv_set(args.include_property_types)
    exclude_property_types = _parse_csv_set(args.exclude_property_types)

    run_timestamp = dt.datetime.now(dt.timezone.utc)
    run_id = run_timestamp.strftime("%Y%m%dT%H%M%SZ")
    output_csv = Path(args.output_csv)
    same_floor_plan_csv = Path(args.same_floor_plan_csv)
    similar_floor_plan_csv = Path(args.similar_floor_plan_csv)
    plan_groups_csv = Path(args.plan_groups_csv)
    evidence_dir = Path(args.evidence_dir)
    evidence_path = evidence_dir / args.dataset_id / f"{run_id}_infer.json"

    try:
        field_names = _load_field_names(args.dataset_id, args.app_token, args.timeout_seconds)
        fields = _detect_fields(field_names)
        address_field = fields.get("address", "")
        property_type_field = fields.get("property_type", "")
        assessed_field = fields.get("assessed_value", "")
        community_field = fields.get("community", "")
        year_field = fields.get("assessment_year", "")
        sqft_field = fields.get("sqft", "")
        roll_field = fields.get("roll_number", "")
        if not address_field or not assessed_field:
            raise RuntimeError("Could not detect required address/assessed value fields.")
        if args.debug:
            print(f"DEBUG: detected fields -> {fields}")
            print(f"DEBUG: subject_unit -> {subject_unit or '(none)'}")

        subject_rows: list[dict[str, Any]] = []
        subject_query_urls: list[str] = []
        subject_queries = _build_subject_queries(subject_address)
        subject_where_clauses = _subject_where_clauses(subject_address, address_field)
        search_mode = str(args.subject_search_mode or "where_only")
        lookup_attempts: list[tuple[str, str]] = []

        if search_mode == "where_only":
            lookup_attempts.extend([("$where", clause) for clause in subject_where_clauses])
            if not lookup_attempts:
                # Fallback when tokenized address cannot build a WHERE clause.
                lookup_attempts.extend([("$q", query) for query in subject_queries])
        elif search_mode == "where_then_q":
            lookup_attempts.extend([("$where", clause) for clause in subject_where_clauses])
            lookup_attempts.extend([("$q", query) for query in subject_queries])
        else:  # q_then_where
            lookup_attempts.extend([("$q", query) for query in subject_queries])
            lookup_attempts.extend([("$where", clause) for clause in subject_where_clauses])

        for operator, value in lookup_attempts:
            base_params = {operator: value}
            rows, urls = _fetch_rows_paginated(
                dataset_id=args.dataset_id,
                base_params=base_params,
                max_rows=args.search_limit,
                page_size=args.page_size,
                app_token=args.app_token,
                timeout_seconds=args.timeout_seconds,
            )
            if args.debug:
                print(f"DEBUG: subject {operator}='{value}' -> {len(rows)} row(s)")
            subject_rows.extend(rows)
            subject_query_urls.extend(urls)
            if rows:
                break
        if not subject_rows:
            raise RuntimeError("No subject rows found. Try a simpler subject address.")
        subject_rows = _dedupe_rows(subject_rows, fields)
        subject_row = _select_subject_row(subject_rows, fields, subject_address, subject_unit)

        subject_property_type = (
            str(subject_row.get(property_type_field, "")).strip().upper()
            if property_type_field
            else ""
        )
        if subject_property_type and subject_property_type in exclude_property_types and not include_property_types:
            exclude_property_types.discard(subject_property_type)

        subject_building = _subject_building_address(subject_address)
        subject_street = _subject_street_portion(subject_building)
        related_civics = _parse_csv_list(args.related_civic_numbers)
        street_discovery_urls: list[str] = []
        if args.all_street_buildings:
            discovered_civics, street_discovery_urls = _discover_street_civics(
                dataset_id=args.dataset_id,
                street_portion=subject_street,
                address_field=address_field,
                app_token=args.app_token,
                timeout_seconds=args.timeout_seconds,
                page_size=args.page_size,
                max_rows=args.street_discovery_max_rows,
                debug=bool(args.debug),
            )
            if discovered_civics:
                related_civics = _sort_civic_tokens(set(related_civics) | set(discovered_civics))
            if args.debug:
                preview = ",".join(related_civics)
                print(f"DEBUG: related civic numbers -> {preview}")
        related_buildings = [f"{civic} {subject_street}" for civic in related_civics if civic]
        extra_buildings = DEFAULT_EXTRA_BUILDINGS[:] if args.include_default_extra_buildings else []
        explicit_buildings = [item for item in args.building_address if item]
        target_buildings = _unique_preserve([subject_building] + related_buildings + extra_buildings + explicit_buildings)

        candidate_entries: list[dict[str, Any]] = []
        candidate_query_urls: list[str] = []
        seen_keys: set[str] = set()
        for target_building in target_buildings:
            clauses = _subject_where_clauses(target_building, address_field)
            if not clauses:
                continue
            for clause in clauses:
                rows, urls = _fetch_rows_paginated(
                    dataset_id=args.dataset_id,
                    base_params={"$where": clause},
                    max_rows=args.max_rows_per_building,
                    page_size=args.page_size,
                    app_token=args.app_token,
                    timeout_seconds=args.timeout_seconds,
                )
                if args.debug:
                    print(f"DEBUG: building='{target_building}' $where='{clause}' -> {len(rows)} row(s)")
                source_url = urls[0] if urls else ""
                candidate_query_urls.extend(urls)
                for row in rows:
                    key = _row_identity_key(row, fields)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    candidate_entries.append(
                        {
                            "target_building": target_building,
                            "row": row,
                            "source_url": source_url,
                        }
                    )

        if not candidate_entries:
            raise RuntimeError("No candidate rows found for related buildings.")

        subject_assessed_value = _to_float(subject_row.get(assessed_field))
        filtered_rows: list[dict[str, str]] = []
        drop_reasons: Counter[str] = Counter()
        raw_by_building: Counter[str] = Counter(entry["target_building"] for entry in candidate_entries)
        kept_by_building: Counter[str] = Counter()
        for entry in candidate_entries:
            row = entry["row"]
            if _same_row(row, subject_row, fields):
                drop_reasons["subject_row"] += 1
                continue

            property_type = (
                str(row.get(property_type_field, "")).strip().upper()
                if property_type_field
                else ""
            )
            assessed_value = _to_float(row.get(assessed_field))
            address_text = str(row.get(address_field, "")).strip()
            unit_token = _extract_row_unit(row, fields)

            if args.match_subject_property_type and subject_property_type and property_type != subject_property_type:
                drop_reasons["match_subject_property_type"] += 1
                continue
            if include_property_types and property_type not in include_property_types:
                drop_reasons["include_property_types"] += 1
                continue
            if exclude_property_types and property_type in exclude_property_types:
                drop_reasons["exclude_property_types"] += 1
                continue
            if assessed_value is not None and assessed_value < args.min_assessed_value:
                drop_reasons["min_assessed_value"] += 1
                continue
            if (not include_virtual_suites) and _is_virtual_suite_record(address_text):
                drop_reasons["virtual_suite"] += 1
                continue

            unit_match, unit_score = _infer_unit_similarity(subject_unit, unit_token)
            metrics = _compute_metric_similarity(subject_row, row)
            metric_similarity_pct = metrics["metric_similarity_pct"]
            metric_fields_matched = set(metrics["metric_fields_matched"])
            floor_plan_relation, relation_reason = _classify_floor_plan_relation(
                unit_match=unit_match,
                unit_score=unit_score,
                metric_similarity_pct=metric_similarity_pct,
                matched_fields=metric_fields_matched,
            )

            value_delta = assessed_value - subject_assessed_value if (
                assessed_value is not None and subject_assessed_value is not None
            ) else None
            value_delta_pct = (value_delta / subject_assessed_value) if (
                value_delta is not None and subject_assessed_value
            ) else None

            filtered_rows.append(
                {
                    "rank": "",
                    "comp_id": "",
                    "source_id": "",
                    "target_building": entry["target_building"],
                    "address": address_text,
                    "unit": unit_token,
                    "subject_unit": subject_unit,
                    "unit_match": unit_match,
                    "unit_score": str(unit_score),
                    "floor_plan_relation": floor_plan_relation,
                    "relation_reason": relation_reason,
                    "plan_signature": _build_plan_signature(row, unit_token),
                    "metric_similarity_pct": f"{metric_similarity_pct:.2f}",
                    "metric_match_count": str(metrics["metric_match_count"]),
                    "metric_total_count": str(metrics["metric_total_count"]),
                    "metric_fields_used": ";".join(metrics["metric_fields_used"]),
                    "metric_fields_matched": ";".join(metrics["metric_fields_matched"]),
                    "community": str(row.get(community_field, "")).strip() if community_field else "",
                    "assessed_value": f"{assessed_value:.0f}" if assessed_value is not None else "",
                    "re_assessed_value": (
                        f"{_to_float(row.get('re_assessed_value')):.0f}"
                        if _to_float(row.get("re_assessed_value")) is not None
                        else ""
                    ),
                    "nr_assessed_value": (
                        f"{_to_float(row.get('nr_assessed_value')):.0f}"
                        if _to_float(row.get("nr_assessed_value")) is not None
                        else ""
                    ),
                    "fl_assessed_value": (
                        f"{_to_float(row.get('fl_assessed_value')):.0f}"
                        if _to_float(row.get("fl_assessed_value")) is not None
                        else ""
                    ),
                    "assessment_year": str(row.get(year_field, "")).strip() if year_field else "",
                    "year_of_construction": str(row.get("year_of_construction", "")).strip(),
                    "property_type": property_type,
                    "assessment_class": str(row.get("assessment_class", "")).strip(),
                    "assessment_class_description": str(row.get("assessment_class_description", "")).strip(),
                    "land_use_designation": str(row.get("land_use_designation", "")).strip(),
                    "sub_property_use": str(row.get("sub_property_use", "")).strip(),
                    "land_size_sm": str(row.get("land_size_sm", "")).strip(),
                    "sqft": str(row.get(sqft_field, "")).strip() if sqft_field else "",
                    "roll_number": str(row.get(roll_field, "")).strip() if roll_field else "",
                    "value_delta": f"{value_delta:.0f}" if value_delta is not None else "",
                    "value_delta_pct": f"{value_delta_pct:.4f}" if value_delta_pct is not None else "",
                    "dataset_id": args.dataset_id,
                    "source_url": entry["source_url"],
                }
            )
            kept_by_building[entry["target_building"]] += 1

        if args.debug and drop_reasons:
            print(f"DEBUG: filter drop reasons -> {dict(drop_reasons)}")
        if args.debug:
            print("DEBUG: per-building rows (raw -> kept)")
            for building in target_buildings:
                raw_count = raw_by_building.get(building, 0)
                kept_count = kept_by_building.get(building, 0)
                print(f"DEBUG:   {building}: {raw_count} -> {kept_count}")

        if not filtered_rows:
            raise RuntimeError("No inferred unit comps remain after filtering.")

        filtered_rows.sort(key=lambda row: _sort_inferred_rows(row, bool(args.prefer_high_end)))
        if args.max_per_stack > 0:
            pre_cap_count = len(filtered_rows)
            filtered_rows = _apply_max_per_stack(filtered_rows, args.max_per_stack)
            if args.debug:
                print(
                    "DEBUG: max_per_stack applied -> "
                    f"kept {len(filtered_rows)} of {pre_cap_count} rows "
                    f"(max_per_stack={args.max_per_stack})"
                )
        if args.max_comps > 0:
            filtered_rows = filtered_rows[: args.max_comps]
        for index, row in enumerate(filtered_rows, start=1):
            row["rank"] = str(index)
            row["comp_id"] = f"C-OCU-{run_id}-{index:03d}"
            row["source_id"] = f"S-OCU-{run_id}-{index:03d}"

        same_floor_plan_rows = [row for row in filtered_rows if row.get("floor_plan_relation") == "same_floor_plan"]
        similar_floor_plan_rows = [row for row in filtered_rows if row.get("floor_plan_relation") == "similar_floor_plan"]
        relation_counts = Counter(row.get("floor_plan_relation", "other") for row in filtered_rows)

        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_payload = {
            "run_id": run_id,
            "dataset_id": args.dataset_id,
            "subject_address": subject_address,
            "subject_unit": subject_unit,
            "subject_row": subject_row,
            "related_civic_numbers": related_civics,
            "target_buildings": target_buildings,
            "detected_fields": fields,
            "query_urls": sorted(set(subject_query_urls + candidate_query_urls + street_discovery_urls)),
            "drop_reasons": dict(drop_reasons),
            "relation_counts": dict(relation_counts),
            "rows_written": len(filtered_rows),
            "captured_at": run_timestamp.isoformat(),
        }
        evidence_path.write_text(json.dumps(evidence_payload, indent=2), encoding="utf-8")

        _write_csv(output_csv, INFERRED_FIELDNAMES, filtered_rows)
        _write_csv(same_floor_plan_csv, INFERRED_FIELDNAMES, same_floor_plan_rows)
        _write_csv(similar_floor_plan_csv, INFERRED_FIELDNAMES, similar_floor_plan_rows)
        _write_plan_groups_csv(plan_groups_csv, filtered_rows)

        print(f"Subject address: {subject_address}")
        print(f"Subject unit token: {subject_unit or '(none)'}")
        print(f"Dataset: {args.dataset_id}")
        print(f"Target buildings: {len(target_buildings)}")
        print(f"Wrote {len(filtered_rows)} rows to {output_csv}")
        print(f"Wrote same-floor-plan rows: {len(same_floor_plan_rows)} -> {same_floor_plan_csv}")
        print(f"Wrote similar-floor-plan rows: {len(similar_floor_plan_rows)} -> {similar_floor_plan_csv}")
        print(f"Wrote plan-group summary: {plan_groups_csv}")
        print(f"Wrote evidence artifact: {evidence_path}")
        if args.debug:
            print(f"DEBUG: relation counts -> {dict(relation_counts)}")
        print("Done.")
        return 0
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
