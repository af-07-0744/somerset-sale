import argparse
import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse

from codex_sale_docs.city_data_fetch import DEFAULT_OUTPUT_META_JSON
from codex_sale_docs.city_data_inventory import DEFAULT_SUMMARY_JSON


DEFAULT_OUTPUT_RST = Path("source/93_city_data_fetch_metadata.rst")
DEFAULT_TABLE_DIR = Path("source/city_data/_tables/meta")
USED_RESPONSE_FIELDS = [
    "address",
    "roll_number",
    "unique_key",
    "cpid",
    "comm_code",
    "comm_name",
    "assessed_value",
    "re_assessed_value",
    "nr_assessed_value",
    "fl_assessed_value",
    "roll_year",
    "property_type",
    "assessment_class",
    "assessment_class_description",
    "land_use_designation",
    "sub_property_use",
    "year_of_construction",
    "land_size_sm",
    "land_size_sf",
    "land_size_ac",
    "mod_date",
]
DEFAULT_FLAT_CSV = Path("data/open_calgary_somervale_raw_rows_flat.csv")
PRICE_FIELDS = {
    "assessed_value",
    "re_assessed_value",
    "nr_assessed_value",
    "fl_assessed_value",
}
INTEGER_ID_FIELDS = {"roll_number", "unique_key", "cpid"}
ADDRESS_FIELDS = {"address"}
YEAR_FIELDS = {"roll_year"}
ENUM_FIELDS = {
    "comm_code",
    "comm_name",
    "property_type",
    "assessment_class",
    "assessment_class_description",
    "land_use_designation",
    "sub_property_use",
}
RANGE_FIELDS = {"year_of_construction", "land_size_sm", "land_size_sf", "land_size_ac"}


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _where_like_tokens(where_clause: str) -> list[str]:
    tokens = [
        _normalize_text(match.group(1)).upper()
        for match in re.finditer(r"(?i)like\s+'%(.+?)%'", where_clause)
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        if (not token) or token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def _request_label_from_where(where_clause: str, stage: str, index: int) -> str:
    tokens = _where_like_tokens(where_clause)
    if tokens:
        return " % ".join(tokens)
    fallback = _normalize_text(where_clause)
    if fallback:
        return fallback
    if stage == "subject_lookup":
        return f"Subject Lookup {index:02d}"
    return f"Street Fetch {index:02d}"


def _url_param_first_values(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    params: dict[str, str] = {}
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key not in params:
            params[key] = value
    return params


def _request_rows(fetch_meta: dict[str, Any]) -> list[dict[str, str]]:
    stage_specs = [
        {
            "stage": "subject_lookup",
            "url_key": "subject_lookup_query_urls",
            "where_key": "subject_lookup_where_clauses",
        },
        {
            "stage": "street_fetch",
            "url_key": "query_urls",
            "where_key": "where_clauses",
        },
    ]
    rows: list[dict[str, str]] = []
    for spec in stage_specs:
        raw_urls = fetch_meta.get(spec["url_key"], [])
        query_urls = [str(url) for url in raw_urls] if isinstance(raw_urls, list) else []
        where_order = fetch_meta.get(spec["where_key"], [])
        ordered_where = [str(value) for value in where_order] if isinstance(where_order, list) else []

        stage_rows: list[dict[str, str]] = []
        for index, url in enumerate(query_urls, start=1):
            params = _url_param_first_values(url)
            where_clause = str(params.get("$where", ""))
            stage_rows.append(
                {
                    "stage": spec["stage"],
                    "where_clause": where_clause,
                    "query_url": str(url),
                    "offset": str(params.get("$offset", "0")),
                    "original_index": str(index),
                }
            )

        if not stage_rows:
            continue

        ordered_rows: list[dict[str, str]] = []
        used: set[int] = set()
        for where_clause in ordered_where:
            matches = [
                row
                for row in stage_rows
                if row["where_clause"] == where_clause and int(row["original_index"]) not in used
            ]
            matches.sort(
                key=lambda row: (
                    int(row["offset"]) if row["offset"].isdigit() else 0,
                    int(row["original_index"]),
                )
            )
            for row in matches:
                used.add(int(row["original_index"]))
                ordered_rows.append(row)
        for row in stage_rows:
            index = int(row["original_index"])
            if index in used:
                continue
            ordered_rows.append(row)

        label_counts: dict[str, int] = {}
        for index, row in enumerate(ordered_rows, start=1):
            label = _request_label_from_where(row["where_clause"], row["stage"], index)
            label_counts[label] = label_counts.get(label, 0) + 1
            suffix = label_counts[label]
            request_name = f"{label} ({suffix})" if suffix > 1 else label
            rows.append(
                {
                    "request": request_name,
                    "stage": row["stage"],
                    "where_clause": row["where_clause"],
                    "query_url": row["query_url"],
                }
            )
    return rows


def _request_names_for_stage(request_rows: list[dict[str, str]], stage: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for row in request_rows:
        if row.get("stage", "") != stage:
            continue
        name = _normalize_text(row.get("request", ""))
        if (not name) or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _request_actor_label(request_names: list[str], fallback: str, preview_limit: int = 3) -> str:
    if not request_names:
        return fallback
    preview = list(request_names[:preview_limit])
    remaining = len(request_names) - len(preview)
    if remaining > 0:
        preview.append(f"(+{remaining} more)")
    return "\\n".join(preview)


def _plantuml_escape(value: str) -> str:
    return str(value).replace('"', '\\"')


def _response_fields_used(fetch_meta: dict[str, Any]) -> list[str]:
    available = {
        _normalize_text(value)
        for value in fetch_meta.get("flat_csv_fieldnames", [])
        if _normalize_text(value)
    }
    selected = [field for field in USED_RESPONSE_FIELDS if field in available]
    return selected


def _derive_flat_csv_path(fetch_meta_json: Path) -> Path:
    text = str(fetch_meta_json)
    suffix = "_raw_rows_meta.json"
    if text.endswith(suffix):
        return Path(f"{text[: -len(suffix)]}_raw_rows_flat.csv")
    return DEFAULT_FLAT_CSV


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _to_float(value: str) -> float | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _format_number(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _sorted_distinct(values: list[str]) -> list[str]:
    distinct = sorted(set(values))
    numeric = [_to_float(value) for value in distinct]
    if all(item is not None for item in numeric):
        return [value for _, value in sorted(zip([float(item) for item in numeric], distinct), key=lambda pair: pair[0])]
    return distinct


def _enum_descriptor(values: list[str]) -> str:
    members = " | ".join(f"\"{value}\"" for value in values)
    return f"{{ {members} }}"


def _integer_digits_descriptor(values: list[str]) -> str | None:
    digit_lengths: list[int] = []
    for value in values:
        text = _normalize_text(value)
        if not text:
            continue
        if re.fullmatch(r"\d+", text):
            digit_lengths.append(len(text))
            continue
        if re.fullmatch(r"\d+\.0+", text):
            digit_lengths.append(len(text.split(".", 1)[0]))
            continue
        return None
    if not digit_lengths:
        return None
    minimum = min(digit_lengths)
    maximum = max(digit_lengths)
    if minimum == maximum:
        return f"integer[{minimum}]"
    return f"integer[{minimum}..{maximum}]"


def _field_descriptor(field: str, values: list[str]) -> str:
    non_empty = [_normalize_text(value) for value in values if _normalize_text(value)]
    if field in PRICE_FIELDS:
        return "price"
    if field in INTEGER_ID_FIELDS:
        return _integer_digits_descriptor(non_empty) or "integer"
    if field in ADDRESS_FIELDS:
        return "text"
    if not non_empty:
        return "text"

    distinct = _sorted_distinct(non_empty)
    numeric_values = [_to_float(value) for value in distinct]
    all_numeric = all(item is not None for item in numeric_values)

    if field in YEAR_FIELDS:
        return _integer_digits_descriptor(non_empty) or "year"
    if field == "mod_date":
        if all(re.fullmatch(r"\d{4}", value) for value in distinct):
            return "year"
        if all(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", value) for value in distinct):
            return "datetime[ISO-8601 UTC]"
        return "datetime"
    if field in ENUM_FIELDS and len(distinct) <= 8:
        return _enum_descriptor(distinct)
    if field in RANGE_FIELDS and all_numeric:
        minimum = min(float(item) for item in numeric_values if item is not None)
        maximum = max(float(item) for item in numeric_values if item is not None)
        return f"{_format_number(minimum)} .. {_format_number(maximum)}"
    if all_numeric:
        minimum = min(float(item) for item in numeric_values if item is not None)
        maximum = max(float(item) for item in numeric_values if item is not None)
        return f"{_format_number(minimum)} .. {_format_number(maximum)}"
    if len(distinct) <= 6:
        return _enum_descriptor(distinct)
    return "text"


def _field_descriptor_lines(fields: list[str], rows: list[dict[str, str]]) -> list[str]:
    lines: list[str] = []
    for field in fields:
        values = [row.get(field, "") for row in rows]
        descriptor = _field_descriptor(field, values)
        lines.append(f"+ {field}: {descriptor}")
    return lines


def _subject_lookup_row_count(
    *,
    fetch_meta: dict[str, Any],
    subject_lookup_label: str,
    rows: list[dict[str, str]],
) -> int | None:
    if not rows:
        return None
    matched_subject = _normalize_text(fetch_meta.get("subject_address_matched", "")).upper()
    if matched_subject:
        matched_count = sum(
            1
            for row in rows
            if _normalize_text(row.get("address", "")).upper() == matched_subject
        )
        if matched_count > 0:
            return matched_count

    tokens = [token.strip().upper() for token in subject_lookup_label.split("%") if token.strip()]
    if not tokens:
        return None
    return sum(
        1
        for row in rows
        if all(token in _normalize_text(row.get("address", "")).upper() for token in tokens)
    )


def _format_row_count(count: int | None) -> str:
    if count is None:
        return "row count unavailable"
    if count == 1:
        return "1 row"
    return f"{count} rows"


def render_city_data_metadata_rst(
    *,
    fetch_meta_json: Path,
    inventory_summary_json: Path,
    output_rst: Path,
    table_dir: Path,
    title: str,
) -> dict[str, Any]:
    if not fetch_meta_json.exists():
        raise RuntimeError(f"Missing fetch metadata JSON: {fetch_meta_json}")

    fetch_meta = _read_json(fetch_meta_json)
    if not isinstance(fetch_meta, dict):
        raise RuntimeError(f"Fetch metadata JSON must contain an object: {fetch_meta_json}")

    inventory_summary: dict[str, Any] = {}
    if inventory_summary_json.exists():
        payload = _read_json(inventory_summary_json)
        if isinstance(payload, dict):
            inventory_summary = payload

    table_dir.mkdir(parents=True, exist_ok=True)
    request_rows = _request_rows(fetch_meta)

    run_summary_rows = [
        {"metric": "Generated At", "value": dt.datetime.now(dt.timezone.utc).isoformat()},
        {"metric": "Dataset ID", "value": str(fetch_meta.get("dataset_id", ""))},
        {"metric": "Run ID", "value": str(fetch_meta.get("run_id", ""))},
        {"metric": "Captured At", "value": str(fetch_meta.get("captured_at", ""))},
        {"metric": "Subject Address", "value": str(fetch_meta.get("subject_address", ""))},
        {"metric": "Subject Address Matched", "value": str(fetch_meta.get("subject_address_matched", ""))},
        {"metric": "Fetch Scope", "value": str(fetch_meta.get("fetch_scope", ""))},
        {"metric": "Street Portion", "value": str(fetch_meta.get("street_portion", ""))},
        {"metric": "Address Field", "value": str(fetch_meta.get("address_field", ""))},
        {"metric": "Rows Raw", "value": str(fetch_meta.get("rows_raw", ""))},
        {"metric": "Rows Filtered", "value": str(fetch_meta.get("rows_filtered", ""))},
        {"metric": "Rows Deduped", "value": str(fetch_meta.get("rows_deduped", ""))},
        {"metric": "Dedupe Enabled", "value": str(fetch_meta.get("dedupe_enabled", ""))},
        {
            "metric": "Subject Lookup Query Count",
            "value": str(len(fetch_meta.get("subject_lookup_query_urls", []))),
        },
        {"metric": "Street Fetch Query Count", "value": str(len(fetch_meta.get("query_urls", [])))},
        {"metric": "Request Count", "value": str(len(request_rows))},
    ]
    run_summary_csv = table_dir / "run_summary.csv"
    _write_csv(run_summary_csv, ["metric", "value"], run_summary_rows)

    subject_where_clauses = fetch_meta.get("subject_lookup_where_clauses", [])
    street_where_clauses = fetch_meta.get("where_clauses", [])
    where_rows = [
        {"stage": "subject_lookup", "index": str(index), "where_clause": str(clause)}
        for index, clause in enumerate(subject_where_clauses, start=1)
    ]
    where_rows.extend(
        {"stage": "street_fetch", "index": str(index), "where_clause": str(clause)}
        for index, clause in enumerate(street_where_clauses, start=1)
    )
    where_csv = table_dir / "where_clauses.csv"
    _write_csv(
        where_csv,
        ["stage", "index", "where_clause"],
        where_rows or [{"stage": "", "index": "", "where_clause": ""}],
    )

    subject_queries = fetch_meta.get("subject_lookup_queries", [])
    subject_query_rows = [{"index": str(index), "subject_query": str(query)} for index, query in enumerate(subject_queries, start=1)]
    subject_queries_csv = table_dir / "subject_lookup_queries.csv"
    _write_csv(
        subject_queries_csv,
        ["index", "subject_query"],
        subject_query_rows or [{"index": "", "subject_query": ""}],
    )

    subject_query_urls = fetch_meta.get("subject_lookup_query_urls", [])
    street_query_urls = fetch_meta.get("query_urls", [])
    query_rows = [
        {"stage": "subject_lookup", "index": str(index), "query_url": str(url)}
        for index, url in enumerate(subject_query_urls, start=1)
    ]
    query_rows.extend(
        {"stage": "street_fetch", "index": str(index), "query_url": str(url)}
        for index, url in enumerate(street_query_urls, start=1)
    )
    query_csv = table_dir / "query_urls.csv"
    _write_csv(
        query_csv,
        ["stage", "index", "query_url"],
        query_rows or [{"stage": "", "index": "", "query_url": ""}],
    )
    subject_lookup_request_count = len(subject_query_urls)
    street_fetch_request_count = len(street_query_urls)

    request_index_csv = table_dir / "request_index.csv"
    _write_csv(
        request_index_csv,
        ["request", "stage", "where_clause", "query_url"],
        request_rows or [{"request": "", "stage": "", "where_clause": "", "query_url": ""}],
    )
    subject_lookup_request_names = _request_names_for_stage(request_rows, "subject_lookup")
    street_fetch_request_names = _request_names_for_stage(request_rows, "street_fetch")
    subject_lookup_actor_label = _request_actor_label(subject_lookup_request_names, "subject_lookup (none)")
    street_fetch_actor_label = _request_actor_label(street_fetch_request_names, "street_fetch (none)")
    used_response_fields = _response_fields_used(fetch_meta)
    flat_csv_path = _derive_flat_csv_path(fetch_meta_json)
    flat_rows = _read_csv_rows(flat_csv_path)
    query_1_field_lines = _field_descriptor_lines(["address"], flat_rows) if "address" in used_response_fields else []
    query_2_field_lines = _field_descriptor_lines(used_response_fields, flat_rows)
    query_1_rows_text = _format_row_count(
        _subject_lookup_row_count(
            fetch_meta=fetch_meta,
            subject_lookup_label=subject_lookup_request_names[0] if subject_lookup_request_names else "",
            rows=flat_rows,
        )
    )
    rows_raw_count = _to_int(fetch_meta.get("rows_raw"))
    rows_deduped_count = _to_int(fetch_meta.get("rows_deduped"))
    if (rows_raw_count is not None) and (rows_deduped_count is not None):
        query_2_rows_text = f"{rows_raw_count} raw / {rows_deduped_count} unique"
    else:
        query_2_rows_text = "row count unavailable"

    inventory_rows = []
    for key in ["rows_total", "rows_condo", "rows_parking", "rows_storage", "rows_other", "distinct_buildings"]:
        if key in inventory_summary:
            inventory_rows.append({"metric": key, "value": str(inventory_summary.get(key, ""))})
    inventory_csv = table_dir / "inventory_summary.csv"
    _write_csv(inventory_csv, ["metric", "value"], inventory_rows or [{"metric": "", "value": ""}])

    lines = [title, "=" * len(title), ""]
    lines.append("Last fetch metadata and generated inventory statistics for City of Calgary data.")
    lines.append("")
    lines.append(".. contents::")
    lines.append("   :local:")
    lines.append("   :depth: 2")
    lines.append("")

    lines.append("Request-to-File Communication Diagram")
    lines.append("-------------------------------------")
    lines.append("")
    lines.append(
        f"- Requests in this run: ``{subject_lookup_request_count}`` subject-lookup + "
        f"``{street_fetch_request_count}`` street-fetch."
    )
    subject_lookup_names_text = (
        ", ".join(f"``{name}``" for name in subject_lookup_request_names)
        if subject_lookup_request_names
        else "``(none)``"
    )
    street_fetch_names_text = (
        ", ".join(f"``{name}``" for name in street_fetch_request_names)
        if street_fetch_request_names
        else "``(none)``"
    )
    lines.append(f"- Subject-lookup labels from request index: {subject_lookup_names_text}")
    lines.append(f"- Street-fetch labels from request index: {street_fetch_names_text}")
    lines.append("- Numbering indicates order; arrows show only query-to-query and query/file-to-file data flow.")
    lines.append("")
    lines.append(".. uml::")
    lines.append("")
    lines.append("   @startuml")
    lines.append("   top to bottom direction")
    lines.append("   allowmixing")
    lines.append("   hide circle")
    lines.append("   skinparam linetype polyline")
    lines.append("   skinparam shadowing false")
    lines.append("   skinparam nodesep 20")
    lines.append("   skinparam ranksep 30")
    lines.append("")
    lines.append(f"   class \"**{_plantuml_escape(subject_lookup_actor_label)}**\" as query_1 <<json>> {{")
    if query_1_field_lines:
        for field_line in query_1_field_lines:
            lines.append(f"     {field_line}")
    else:
        lines.append("     + address: text")
    lines.append("     ..")
    lines.append(f"     {query_1_rows_text}")
    lines.append("     ==")
    lines.append("     Subject lookup query")
    lines.append("   }")
    lines.append(f"   class \"**{_plantuml_escape(street_fetch_actor_label)}**\" as query_2 <<json>> {{")
    if query_2_field_lines:
        for field_line in query_2_field_lines:
            lines.append(f"     {field_line}")
    else:
        lines.append("     + (none detected)")
    lines.append("     ..")
    lines.append(f"     {query_2_rows_text}")
    lines.append("     ==")
    lines.append("     Street fetch query")
    lines.append("   }")
    lines.append("   file \"data/open_calgary_somervale_raw_rows.json\" as raw_json")
    lines.append("   file \"data/open_calgary_somervale_raw_rows_flat.csv\" as flat_csv")
    lines.append("   file \"data/open_calgary_somervale_raw_\\nfield_profile.csv\" as field_profile")
    lines.append("   file \"data/open_calgary_somervale_inventory_\\nsummary.json\" as inventory_summary")
    lines.append("   file \"data/open_calgary_street_requested_\\nfield_dictionary.csv\" as enum_dictionary")
    lines.append("   file \"source/92_city_data_enum_dictionary.rst\" as enum_rst")
    lines.append("   file \"source/91_city_data_index.rst\\nsource/city_data/building_*.rst\" as city_pages")
    lines.append("   file \"source/93_city_data_fetch_metadata.rst\" as metadata_rst")
    lines.append("")
    lines.append("   query_1 --> query_2 : 1) street_portion")
    lines.append("")
    lines.append("   query_2 --> raw_json")
    lines.append("   raw_json -down-> flat_csv : 3) flattened rows")
    lines.append("   flat_csv -down-> field_profile : 4) field profile")
    lines.append("")
    lines.append("   flat_csv -down-> inventory_summary : 5) inventory output")
    lines.append("   flat_csv -right-> enum_dictionary : 6) enum dictionary")
    lines.append("   enum_dictionary -down-> enum_rst : 7) dictionary source")
    lines.append("")
    lines.append("   flat_csv -right-> city_pages : 8) page rows")
    lines.append("   raw_json -right-> city_pages : 9) provenance")
    lines.append("")
    lines.append("   inventory_summary -down-> metadata_rst : 10) inventory input")
    lines.append("   field_profile -[hidden]right-> inventory_summary")
    lines.append("   inventory_summary -[hidden]right-> enum_dictionary")
    lines.append("   enum_dictionary -[hidden]right-> city_pages")
    lines.append("   @enduml")
    lines.append("")

    lines.append("Fetch Summary")
    lines.append("-------------")
    lines.append("")
    lines.append(".. csv-table:: Run Summary")
    lines.append(f"   :file: city_data/_tables/meta/{run_summary_csv.name}")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")

    lines.append("Where Clauses")
    lines.append("-------------")
    lines.append("")
    lines.append(".. csv-table:: Request WHERE Clauses")
    lines.append(f"   :file: city_data/_tables/meta/{where_csv.name}")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")

    lines.append("Subject Lookup Queries")
    lines.append("----------------------")
    lines.append("")
    lines.append(".. csv-table:: Subject Lookup Query Attempts")
    lines.append(f"   :file: city_data/_tables/meta/{subject_queries_csv.name}")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")

    lines.append("Request Index")
    lines.append("-------------")
    lines.append("")
    lines.append(".. csv-table:: Request Index")
    lines.append(f"   :file: city_data/_tables/meta/{request_index_csv.name}")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")

    lines.append("Query URLs")
    lines.append("----------")
    lines.append("")
    lines.append(".. csv-table:: Query URLs Used")
    lines.append(f"   :file: city_data/_tables/meta/{query_csv.name}")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")

    lines.append("Inventory Summary")
    lines.append("-----------------")
    lines.append("")
    lines.append(f"- Inventory summary JSON: ``{inventory_summary_json}``")
    lines.append("")
    lines.append(".. csv-table:: Inventory Bucket Summary")
    lines.append(f"   :file: city_data/_tables/meta/{inventory_csv.name}")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")

    output_rst.parent.mkdir(parents=True, exist_ok=True)
    output_rst.write_text("\n".join(lines), encoding="utf-8")
    return {
        "output_rst": str(output_rst),
        "run_summary_csv": str(run_summary_csv),
        "where_clauses_csv": str(where_csv),
        "subject_lookup_queries_csv": str(subject_queries_csv),
        "request_index_csv": str(request_index_csv),
        "query_urls_csv": str(query_csv),
        "inventory_summary_csv": str(inventory_csv),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render a dedicated city-data metadata page with last fetch details and inventory summary."
    )
    parser.add_argument("--fetch-meta-json", default=str(DEFAULT_OUTPUT_META_JSON), help="Fetch metadata JSON path.")
    parser.add_argument(
        "--inventory-summary-json",
        default=str(DEFAULT_SUMMARY_JSON),
        help="Inventory summary JSON path.",
    )
    parser.add_argument("--output-rst", default=str(DEFAULT_OUTPUT_RST), help="Output metadata rST path.")
    parser.add_argument("--table-dir", default=str(DEFAULT_TABLE_DIR), help="Output CSV-table directory.")
    parser.add_argument("--title", default="City Data Fetch Metadata", help="Metadata page title.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = render_city_data_metadata_rst(
            fetch_meta_json=Path(args.fetch_meta_json),
            inventory_summary_json=Path(args.inventory_summary_json),
            output_rst=Path(args.output_rst),
            table_dir=Path(args.table_dir),
            title=args.title,
        )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1

    print(f"Wrote metadata page: {result['output_rst']}")
    print(f"Wrote run summary table: {result['run_summary_csv']}")
    print(f"Wrote where-clauses table: {result['where_clauses_csv']}")
    print(f"Wrote subject-lookup-queries table: {result['subject_lookup_queries_csv']}")
    print(f"Wrote request-index table: {result['request_index_csv']}")
    print(f"Wrote query-urls table: {result['query_urls_csv']}")
    print(f"Wrote inventory summary table: {result['inventory_summary_csv']}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
