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

    request_index_csv = table_dir / "request_index.csv"
    _write_csv(
        request_index_csv,
        ["request", "stage", "where_clause", "query_url"],
        request_rows or [{"request": "", "stage": "", "where_clause": "", "query_url": ""}],
    )

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
