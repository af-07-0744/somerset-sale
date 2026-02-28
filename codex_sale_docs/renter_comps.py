import argparse
import csv
import datetime as dt
import os
from pathlib import Path
from typing import Any


DEFAULT_INPUT_CSV = Path("data/open_calgary_assessment_comps.csv")
DEFAULT_OUTPUT_CSV = Path("data/renter_comps_table.csv")
DEFAULT_OUTPUT_RST = Path("source/03b_renter_comps_generated.rst")
DEFAULT_STRETCH_OUTPUT_CSV = Path("data/renter_comps_stretch_table.csv")
DEFAULT_STRETCH_OUTPUT_RST = Path("source/03c_renter_comps_stretch_generated.rst")

OUTPUT_FIELDNAMES = [
    "Rank",
    "Address",
    "Community",
    "Assessment Value (CAD)",
    "Difference vs Subject (CAD)",
    "Difference vs Subject (%)",
    "Property Type",
    "Sqft",
    "Source ID",
]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _format_currency(value: float | None) -> str:
    if value is None:
        return ""
    if value < 0:
        return f"-${abs(value):,.0f}"
    return f"${value:,.0f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value * 100:+.2f}%"


def _load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise RuntimeError(f"Missing input file: {path}")
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise RuntimeError(f"No data rows in input file: {path}")
    return rows


def _parse_csv_set(raw_value: str) -> set[str]:
    return {item.strip().lower() for item in raw_value.split(",") if item.strip()}


def _sort_key(row: dict[str, str], prefer_high_end: bool) -> tuple[float, float, float]:
    delta_pct = _to_float(row.get("value_delta_pct"))
    delta_abs = abs(delta_pct) if delta_pct is not None else 10**9
    assessed = _to_float(row.get("assessed_value")) or 0.0
    if not prefer_high_end:
        return (delta_abs, -assessed, 0.0)
    non_negative_tier = 0.0 if delta_pct is not None and delta_pct >= 0 else 1.0
    high_end_bias = -(delta_pct if delta_pct is not None else -10**9)
    return (non_negative_tier, high_end_bias, -assessed)


def _prepare_rows(
    rows: list[dict[str, str]],
    top_n: int,
    prefer_high_end: bool,
    min_value_delta_pct: float | None,
    max_value_delta_pct: float | None,
    exclude_negative_deltas: bool,
    min_unit_score: float,
    allow_unit_match: set[str],
) -> list[dict[str, str]]:
    filtered_rows: list[dict[str, str]] = []
    for row in rows:
        delta_pct = _to_float(row.get("value_delta_pct"))
        unit_score = _to_float(row.get("unit_score"))
        unit_match = (row.get("unit_match", "") or "").strip().lower()

        if exclude_negative_deltas and delta_pct is not None and delta_pct < 0:
            continue
        if min_value_delta_pct is not None and (delta_pct is None or delta_pct < min_value_delta_pct):
            continue
        if max_value_delta_pct is not None and (delta_pct is None or delta_pct > max_value_delta_pct):
            continue
        if min_unit_score > 0 and (unit_score is None or unit_score < min_unit_score):
            continue
        if allow_unit_match and unit_match not in allow_unit_match:
            continue
        filtered_rows.append(row)

    sorted_rows = sorted(filtered_rows, key=lambda row: _sort_key(row, prefer_high_end))[:top_n]
    output_rows: list[dict[str, str]] = []
    for index, row in enumerate(sorted_rows, start=1):
        assessed_value = _to_float(row.get("assessed_value"))
        delta_value = _to_float(row.get("value_delta"))
        delta_pct = _to_float(row.get("value_delta_pct"))
        output_rows.append(
            {
                "Rank": str(index),
                "Address": row.get("address", ""),
                "Community": row.get("community", ""),
                "Assessment Value (CAD)": _format_currency(assessed_value),
                "Difference vs Subject (CAD)": _format_currency(delta_value),
                "Difference vs Subject (%)": _format_pct(delta_pct),
                "Property Type": row.get("property_type", ""),
                "Sqft": row.get("sqft", ""),
                "Source ID": row.get("source_id", ""),
            }
        )
    return output_rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_filters_summary(
    exclude_negative_deltas: bool,
    min_value_delta_pct: float | None,
    max_value_delta_pct: float | None,
    min_unit_score: float,
    allow_unit_match: set[str],
) -> str:
    filters_applied: list[str] = []
    if exclude_negative_deltas:
        filters_applied.append("exclude_negative_deltas")
    if min_value_delta_pct is not None:
        filters_applied.append(f"min_value_delta_pct={min_value_delta_pct:.4f}")
    if max_value_delta_pct is not None:
        filters_applied.append(f"max_value_delta_pct={max_value_delta_pct:.4f}")
    if min_unit_score > 0:
        filters_applied.append(f"min_unit_score={min_unit_score:.0f}")
    if allow_unit_match:
        filters_applied.append("allow_unit_match=" + ",".join(sorted(allow_unit_match)))
    return "; ".join(filters_applied) if filters_applied else "none"


def _build_table_rst(
    title: str,
    intro_lines: list[str],
    table_caption: str,
    input_csv_path: Path,
    output_csv_path: Path,
    output_rst_path: Path,
    included_rows: int,
    dataset_ids: list[str],
    row_count: int,
    ranking_mode: str,
    filters_summary: str,
) -> str:
    dataset_text = ", ".join(dataset_ids) if dataset_ids else "unknown"
    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    csv_path_for_rst = os.path.relpath(output_csv_path, output_rst_path.parent).replace("\\", "/")
    heading_underline = "=" * len(title)
    lines = [
        title,
        heading_underline,
        "",
        *intro_lines,
        "",
        f"- Source dataset(s): ``{dataset_text}``",
        f"- Input rows scanned: ``{row_count}``",
        f"- Rows included in this table: ``{included_rows}``",
        f"- Ranking mode: ``{ranking_mode}``",
        f"- Filters: ``{filters_summary}``",
        f"- Generated at: ``{generated_at}``",
        f"- Input file: ``{input_csv_path}``",
        "",
        f".. csv-table:: {table_caption}",
        f"   :file: {csv_path_for_rst}",
        "   :header-rows: 1",
        "",
        "Source details and provenance IDs are listed in :doc:`90_appendix_sources`.",
        "",
    ]
    return "\n".join(lines)


def _write_rst(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate renter-facing comparable table files from "
            "data/open_calgary_assessment_comps.csv."
        )
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    parser.add_argument("--output-rst", default=str(DEFAULT_OUTPUT_RST))
    parser.add_argument("--top-n", type=int, default=12)
    parser.add_argument(
        "--prefer-high-end",
        action="store_true",
        help="Prioritize comps with non-negative value deltas before lower comps.",
    )
    parser.add_argument(
        "--exclude-negative-deltas",
        action="store_true",
        help="Exclude rows where value_delta_pct is negative.",
    )
    parser.add_argument(
        "--min-value-delta-pct",
        type=float,
        default=None,
        help="Minimum value_delta_pct (decimal form, e.g. 0.00 or 0.02).",
    )
    parser.add_argument(
        "--max-value-delta-pct",
        type=float,
        default=None,
        help="Maximum value_delta_pct (decimal form, e.g. 0.10).",
    )
    parser.add_argument(
        "--min-unit-score",
        type=float,
        default=0.0,
        help="Minimum inferred unit_score to include (when present).",
    )
    parser.add_argument(
        "--allow-unit-match",
        default="",
        help="Comma-delimited unit_match allow-list (e.g. same_unit,same_stack+adjacent_floor).",
    )
    parser.add_argument(
        "--generate-stretch-table",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Generate a separate stretch comparable table page (default: enabled).",
    )
    parser.add_argument("--stretch-output-csv", default=str(DEFAULT_STRETCH_OUTPUT_CSV))
    parser.add_argument("--stretch-output-rst", default=str(DEFAULT_STRETCH_OUTPUT_RST))
    parser.add_argument("--stretch-top-n", type=int, default=8)
    parser.add_argument(
        "--stretch-prefer-high-end",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use high-end ordering for stretch comps (default: enabled).",
    )
    parser.add_argument(
        "--stretch-exclude-negative-deltas",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exclude negative deltas in stretch table (default: enabled).",
    )
    parser.add_argument("--stretch-min-value-delta-pct", type=float, default=0.05)
    parser.add_argument("--stretch-max-value-delta-pct", type=float, default=None)
    parser.add_argument("--stretch-min-unit-score", type=float, default=40.0)
    parser.add_argument(
        "--stretch-allow-unit-match",
        default="same_floor",
        help="Comma-delimited unit_match allow-list for stretch table.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)
    output_rst = Path(args.output_rst)
    top_n = max(1, args.top_n)
    prefer_high_end = bool(args.prefer_high_end)
    exclude_negative_deltas = bool(args.exclude_negative_deltas)
    min_value_delta_pct = args.min_value_delta_pct
    max_value_delta_pct = args.max_value_delta_pct
    min_unit_score = float(args.min_unit_score)
    allow_unit_match = _parse_csv_set(args.allow_unit_match)
    generate_stretch_table = bool(args.generate_stretch_table)
    stretch_output_csv = Path(args.stretch_output_csv)
    stretch_output_rst = Path(args.stretch_output_rst)
    stretch_top_n = max(1, args.stretch_top_n)
    stretch_prefer_high_end = bool(args.stretch_prefer_high_end)
    stretch_exclude_negative_deltas = bool(args.stretch_exclude_negative_deltas)
    stretch_min_value_delta_pct = args.stretch_min_value_delta_pct
    stretch_max_value_delta_pct = args.stretch_max_value_delta_pct
    stretch_min_unit_score = float(args.stretch_min_unit_score)
    stretch_allow_unit_match = _parse_csv_set(args.stretch_allow_unit_match)

    try:
        source_rows = _load_rows(input_csv)
        output_rows = _prepare_rows(
            source_rows,
            top_n,
            prefer_high_end,
            min_value_delta_pct,
            max_value_delta_pct,
            exclude_negative_deltas,
            min_unit_score,
            allow_unit_match,
        )
        _write_csv(output_csv, output_rows)

        dataset_ids = sorted({row.get("dataset_id", "").strip() for row in source_rows if row.get("dataset_id")})
        filters_summary = _build_filters_summary(
            exclude_negative_deltas=exclude_negative_deltas,
            min_value_delta_pct=min_value_delta_pct,
            max_value_delta_pct=max_value_delta_pct,
            min_unit_score=min_unit_score,
            allow_unit_match=allow_unit_match,
        )
        ranking_mode = "high-end first (non-negative deltas prioritized)" if prefer_high_end else "closest to subject"
        rst_content = _build_table_rst(
            title="Renter-Facing Comparable Table",
            intro_lines=[
                "This page is auto-generated from open-data assessment proxies.",
                "It is useful for price positioning, but it is not a substitute for MLS sold comparables.",
            ],
            table_caption="Assessment Proxy Comparables (Open Data)",
            input_csv_path=input_csv,
            output_csv_path=output_csv,
            output_rst_path=output_rst,
            included_rows=len(output_rows),
            dataset_ids=dataset_ids,
            row_count=len(source_rows),
            ranking_mode=ranking_mode,
            filters_summary=filters_summary,
        )
        _write_rst(output_rst, rst_content)

        stretch_rows: list[dict[str, str]] = []
        if generate_stretch_table:
            stretch_rows = _prepare_rows(
                source_rows,
                stretch_top_n,
                stretch_prefer_high_end,
                stretch_min_value_delta_pct,
                stretch_max_value_delta_pct,
                stretch_exclude_negative_deltas,
                stretch_min_unit_score,
                stretch_allow_unit_match,
            )
            _write_csv(stretch_output_csv, stretch_rows)
            stretch_filters_summary = _build_filters_summary(
                exclude_negative_deltas=stretch_exclude_negative_deltas,
                min_value_delta_pct=stretch_min_value_delta_pct,
                max_value_delta_pct=stretch_max_value_delta_pct,
                min_unit_score=stretch_min_unit_score,
                allow_unit_match=stretch_allow_unit_match,
            )
            stretch_ranking_mode = (
                "high-end first (non-negative deltas prioritized)"
                if stretch_prefer_high_end
                else "closest to subject"
            )
            stretch_rst = _build_table_rst(
                title="Stretch Comparable Table",
                intro_lines=[
                    "This page is an optimistic stretch scenario using open-data assessment proxies.",
                    "Use it as a secondary anchor set; primary pricing should rely on the stricter renter-facing table.",
                ],
                table_caption="Stretch Assessment Proxy Comparables (Open Data)",
                input_csv_path=input_csv,
                output_csv_path=stretch_output_csv,
                output_rst_path=stretch_output_rst,
                included_rows=len(stretch_rows),
                dataset_ids=dataset_ids,
                row_count=len(source_rows),
                ranking_mode=stretch_ranking_mode,
                filters_summary=stretch_filters_summary,
            )
            _write_rst(stretch_output_rst, stretch_rst)

        print(f"Wrote renter table CSV: {output_csv}")
        print(f"Wrote renter table RST: {output_rst}")
        print(f"Rows exported: {len(output_rows)}")
        if generate_stretch_table:
            print(f"Wrote stretch table CSV: {stretch_output_csv}")
            print(f"Wrote stretch table RST: {stretch_output_rst}")
            print(f"Stretch rows exported: {len(stretch_rows)}")
        return 0
    except Exception as exc:  # pragma: no cover - CLI entry
        print(f"ERROR: {exc}")
        if "Missing input file" in str(exc):
            print('Hint: run "poetry run fetch-open-calgary --subject-address \\"<address>\\"" first.')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
