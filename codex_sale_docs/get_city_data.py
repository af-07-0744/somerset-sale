import argparse
import os
from pathlib import Path

from codex_sale_docs.city_data_fetch import (
    DEFAULT_OUTPUT_FIELD_PROFILE_CSV,
    DEFAULT_OUTPUT_FLAT_CSV,
    DEFAULT_OUTPUT_JSON,
    DEFAULT_OUTPUT_META_JSON,
    DEFAULT_SUBJECT_SEARCH_LIMIT,
    DEFAULT_STREET_PORTION,
    DEFAULT_SUBJECT_ADDRESS,
    fetch_city_data,
)
from codex_sale_docs.city_data_enums import (
    DEFAULT_DOWNLOAD_DIR as DEFAULT_ENUM_DOWNLOAD_DIR,
    DEFAULT_EXCLUDE_FIELDS as DEFAULT_ENUM_EXCLUDE_FIELDS,
    DEFAULT_MAX_DISTINCT as DEFAULT_ENUM_MAX_DISTINCT,
    DEFAULT_OUTPUT_CSV as DEFAULT_ENUM_OUTPUT_CSV,
    DEFAULT_OUTPUT_RST as DEFAULT_ENUM_OUTPUT_RST,
    DEFAULT_RST_TABLE_DIR as DEFAULT_ENUM_RST_TABLE_DIR,
    DEFAULT_RST_TITLE as DEFAULT_ENUM_RST_TITLE,
    build_enum_dictionary,
)
from codex_sale_docs.city_data_inventory import (
    DEFAULT_ALL_PROPERTIES_CSV,
    DEFAULT_BUILDING_SUMMARY_CSV,
    DEFAULT_CONDO_UNITS_CSV,
    DEFAULT_OTHER_PROPERTIES_CSV,
    DEFAULT_PARKING_UNITS_CSV,
    DEFAULT_STORAGE_UNITS_CSV,
    DEFAULT_SUB_PROPERTY_USE_COUNTS_CSV,
    DEFAULT_SUBJECT_PROFILE_CSV,
    DEFAULT_SUBJECT_PROFILE_JSON,
    DEFAULT_SUMMARY_JSON,
    DEFAULT_UNIT_LINK_INDEX_CSV,
    build_city_data_inventory,
)
from codex_sale_docs.city_data_metadata_rst import (
    DEFAULT_OUTPUT_RST as DEFAULT_METADATA_OUTPUT_RST,
    DEFAULT_TABLE_DIR as DEFAULT_METADATA_TABLE_DIR,
    render_city_data_metadata_rst,
)
from codex_sale_docs.city_data_rst import (
    DEFAULT_INDEX_RST,
    DEFAULT_INDEX_TITLE,
    DEFAULT_NON_RESIDENTIAL_TITLE,
    DEFAULT_OUTPUT_DIR,
    render_city_data_rst,
)
from codex_sale_docs.open_calgary import DEFAULT_DATASET_ID, _normalize_space


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch subject-resolved street City of Calgary data, build a comprehensive inventory, "
            "and generate city-data rST + metadata pages in one command."
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
    parser.add_argument("--street-portion", default=DEFAULT_STREET_PORTION, help="Street portion override.")
    parser.add_argument(
        "--extra-where-clause",
        action="append",
        default=[],
        help="Additional raw SoQL $where clause (repeatable).",
    )
    parser.add_argument("--address-field", default="", help="Optional address field override.")
    parser.add_argument("--no-dedupe", action="store_true", help="Disable dedupe in fetch output.")
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON), help="Raw rows JSON output path.")
    parser.add_argument("--output-flat-csv", default=str(DEFAULT_OUTPUT_FLAT_CSV), help="Flat CSV output path.")
    parser.add_argument("--output-meta-json", default=str(DEFAULT_OUTPUT_META_JSON), help="Fetch metadata JSON output path.")
    parser.add_argument(
        "--output-field-profile-csv",
        default=str(DEFAULT_OUTPUT_FIELD_PROFILE_CSV),
        help="Field coverage CSV output path.",
    )

    parser.add_argument("--skip-inventory", action="store_true", help="Skip inventory generation step.")
    parser.add_argument(
        "--inventory-input-csv",
        default="",
        help="Inventory input CSV. Defaults to --output-flat-csv.",
    )
    parser.add_argument(
        "--inventory-community-name",
        default="",
        help="Optional inventory comm_name filter. Leave blank to keep full street-scope dataset.",
    )
    parser.add_argument(
        "--inventory-all-properties-csv",
        default=str(DEFAULT_ALL_PROPERTIES_CSV),
        help="Inventory all-properties CSV output path.",
    )
    parser.add_argument(
        "--inventory-condo-units-csv",
        default=str(DEFAULT_CONDO_UNITS_CSV),
        help="Inventory condo-units CSV output path.",
    )
    parser.add_argument(
        "--inventory-parking-units-csv",
        default=str(DEFAULT_PARKING_UNITS_CSV),
        help="Inventory parking-units CSV output path.",
    )
    parser.add_argument(
        "--inventory-storage-units-csv",
        default=str(DEFAULT_STORAGE_UNITS_CSV),
        help="Inventory storage-units CSV output path.",
    )
    parser.add_argument(
        "--inventory-other-properties-csv",
        default=str(DEFAULT_OTHER_PROPERTIES_CSV),
        help="Inventory other-properties CSV output path.",
    )
    parser.add_argument(
        "--inventory-unit-link-index-csv",
        default=str(DEFAULT_UNIT_LINK_INDEX_CSV),
        help="Inventory unit-link index CSV output path.",
    )
    parser.add_argument(
        "--inventory-subject-profile-json",
        default=str(DEFAULT_SUBJECT_PROFILE_JSON),
        help="Inventory subject profile JSON output path.",
    )
    parser.add_argument(
        "--inventory-subject-profile-csv",
        default=str(DEFAULT_SUBJECT_PROFILE_CSV),
        help="Inventory subject profile CSV output path.",
    )
    parser.add_argument(
        "--inventory-building-summary-csv",
        default=str(DEFAULT_BUILDING_SUMMARY_CSV),
        help="Inventory building summary CSV output path.",
    )
    parser.add_argument(
        "--inventory-sub-property-use-counts-csv",
        default=str(DEFAULT_SUB_PROPERTY_USE_COUNTS_CSV),
        help="Inventory building/sub_property_use counts CSV output path.",
    )
    parser.add_argument(
        "--inventory-summary-json",
        default=str(DEFAULT_SUMMARY_JSON),
        help="Inventory summary JSON output path.",
    )

    parser.add_argument("--rst-input-csv", default="", help="Input CSV for rST generation. Defaults to --output-flat-csv.")
    parser.add_argument("--rst-output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for generated rST pages.")
    parser.add_argument("--rst-index-rst", default=str(DEFAULT_INDEX_RST), help="Top-level city-data index rST path.")
    parser.add_argument(
        "--rst-raw-json-path",
        default="",
        help="Raw JSON path shown in the generated index note. Defaults to --output-json.",
    )
    parser.add_argument(
        "--rst-fetch-meta-json",
        default="",
        help="Fetch metadata JSON path used for subject quick links. Defaults to --output-meta-json.",
    )
    parser.add_argument(
        "--rst-include-multipolygon",
        action="store_true",
        help="Include full multipolygon geometry field in generated tables.",
    )
    parser.add_argument("--rst-index-title", default=DEFAULT_INDEX_TITLE, help="Title for city-data index page.")
    parser.add_argument(
        "--rst-non-residential-title",
        default=DEFAULT_NON_RESIDENTIAL_TITLE,
        help="Title for non-residential / exceptional records page.",
    )

    parser.add_argument("--skip-metadata-rst", action="store_true", help="Skip metadata rST generation step.")
    parser.add_argument(
        "--metadata-fetch-meta-json",
        default="",
        help="Metadata-page fetch meta JSON path. Defaults to --output-meta-json.",
    )
    parser.add_argument(
        "--metadata-inventory-summary-json",
        default="",
        help="Metadata-page inventory summary JSON path. Defaults to --inventory-summary-json.",
    )
    parser.add_argument(
        "--metadata-output-rst",
        default=str(DEFAULT_METADATA_OUTPUT_RST),
        help="Metadata-page output rST path.",
    )
    parser.add_argument(
        "--metadata-table-dir",
        default=str(DEFAULT_METADATA_TABLE_DIR),
        help="Metadata-page CSV-table output directory.",
    )
    parser.add_argument(
        "--metadata-title",
        default="City Data Fetch Metadata",
        help="Metadata-page title.",
    )

    parser.add_argument("--skip-enums", action="store_true", help="Skip enum dictionary generation step.")
    parser.add_argument(
        "--enums-input-csv",
        default="",
        help="Enum input CSV path. Defaults to --output-flat-csv.",
    )
    parser.add_argument(
        "--enums-output-csv",
        default=str(DEFAULT_ENUM_OUTPUT_CSV),
        help="Enum dictionary CSV output path.",
    )
    parser.add_argument(
        "--enums-output-rst",
        default=str(DEFAULT_ENUM_OUTPUT_RST),
        help="Enum dictionary rST output path.",
    )
    parser.add_argument(
        "--enums-run-meta-json",
        default="",
        help="Enum run-meta JSON path. Defaults to --output-meta-json.",
    )
    parser.add_argument(
        "--enums-rst-table-dir",
        default=str(DEFAULT_ENUM_RST_TABLE_DIR),
        help="Enum table directory path.",
    )
    parser.add_argument(
        "--enums-download-dir",
        default=str(DEFAULT_ENUM_DOWNLOAD_DIR),
        help="Enum download artifact directory path.",
    )
    parser.add_argument(
        "--enums-title",
        default=DEFAULT_ENUM_RST_TITLE,
        help="Enum dictionary rST title.",
    )
    parser.add_argument(
        "--enums-include-blank",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include explicit blank rows in enum output (default: enabled).",
    )

    parser.add_argument("--skip-fetch", action="store_true", help="Skip fetch step and run rST generation only.")
    parser.add_argument("--skip-rst", action="store_true", help="Skip rST generation step and run fetch only.")
    parser.add_argument("--debug", action="store_true", help="Print debug details during fetch.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.skip_fetch and args.skip_inventory and args.skip_rst and args.skip_metadata_rst and args.skip_enums:
        print("ERROR: nothing to do; all steps were skipped.")
        return 1

    try:
        if not args.skip_fetch:
            fetch_result = fetch_city_data(
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
            print(
                "Fetch complete: "
                f"rows_raw={fetch_result['rows_raw']} rows_deduped={fetch_result['rows_deduped']} "
                f"flat_csv={args.output_flat_csv}"
            )

        if not args.skip_inventory:
            inventory_input_csv = Path(args.inventory_input_csv) if args.inventory_input_csv.strip() else Path(
                args.output_flat_csv
            )
            inventory_result = build_city_data_inventory(
                input_csv=inventory_input_csv,
                subject_address=_normalize_space(args.subject_address),
                community_name=args.inventory_community_name,
                all_properties_csv=Path(args.inventory_all_properties_csv),
                condo_units_csv=Path(args.inventory_condo_units_csv),
                parking_units_csv=Path(args.inventory_parking_units_csv),
                storage_units_csv=Path(args.inventory_storage_units_csv),
                other_properties_csv=Path(args.inventory_other_properties_csv),
                unit_link_index_csv=Path(args.inventory_unit_link_index_csv),
                subject_profile_json=Path(args.inventory_subject_profile_json),
                subject_profile_csv=Path(args.inventory_subject_profile_csv),
                building_summary_csv=Path(args.inventory_building_summary_csv),
                sub_property_use_counts_csv=Path(args.inventory_sub_property_use_counts_csv),
                summary_json=Path(args.inventory_summary_json),
            )
            print(
                "Inventory generation complete: "
                f"rows_total={inventory_result['rows_total']} "
                f"condo={inventory_result['rows_condo']} "
                f"parking={inventory_result['rows_parking']} "
                f"storage={inventory_result['rows_storage']} "
                f"other={inventory_result['rows_other']}"
            )

        if not args.skip_rst:
            rst_input_csv = Path(args.rst_input_csv) if args.rst_input_csv.strip() else Path(args.output_flat_csv)
            rst_raw_json = Path(args.rst_raw_json_path) if args.rst_raw_json_path.strip() else Path(args.output_json)
            rst_fetch_meta_json = (
                Path(args.rst_fetch_meta_json) if args.rst_fetch_meta_json.strip() else Path(args.output_meta_json)
            )
            rst_result = render_city_data_rst(
                input_csv=rst_input_csv,
                output_dir=Path(args.rst_output_dir),
                index_rst=Path(args.rst_index_rst),
                raw_json_path=rst_raw_json,
                fetch_meta_json=rst_fetch_meta_json,
                include_multipolygon=bool(args.rst_include_multipolygon),
                index_title=args.rst_index_title,
                non_residential_title=args.rst_non_residential_title,
            )
            print(
                "rST generation complete: "
                f"building_pages={rst_result['building_pages']} index={rst_result['index_rst']}"
            )

        if not args.skip_metadata_rst:
            fetch_meta_path = (
                Path(args.metadata_fetch_meta_json) if args.metadata_fetch_meta_json.strip() else Path(args.output_meta_json)
            )
            inventory_summary_path = (
                Path(args.metadata_inventory_summary_json)
                if args.metadata_inventory_summary_json.strip()
                else Path(args.inventory_summary_json)
            )
            metadata_result = render_city_data_metadata_rst(
                fetch_meta_json=fetch_meta_path,
                inventory_summary_json=inventory_summary_path,
                output_rst=Path(args.metadata_output_rst),
                table_dir=Path(args.metadata_table_dir),
                title=args.metadata_title,
            )
            print(
                "Metadata rST generation complete: "
                f"metadata_rst={metadata_result['output_rst']}"
            )

        if not args.skip_enums:
            enums_input_csv = Path(args.enums_input_csv) if args.enums_input_csv.strip() else Path(args.output_flat_csv)
            enums_meta_json = Path(args.enums_run_meta_json) if args.enums_run_meta_json.strip() else Path(
                args.output_meta_json
            )
            enums_result = build_enum_dictionary(
                input_csv=enums_input_csv,
                output_csv=Path(args.enums_output_csv),
                output_rst=Path(args.enums_output_rst),
                rst_table_dir=Path(args.enums_rst_table_dir),
                download_dir=Path(args.enums_download_dir),
                rst_title=args.enums_title,
                run_meta_json=enums_meta_json,
                max_distinct=DEFAULT_ENUM_MAX_DISTINCT,
                include_numeric=False,
                auto_fields=False,
                include_fields=[],
                exclude_fields=set(DEFAULT_ENUM_EXCLUDE_FIELDS),
                include_blank=bool(args.enums_include_blank),
                debug=bool(args.debug),
            )
            print(
                "Enum generation complete: "
                f"fields_selected={enums_result['fields_selected']} "
                f"dictionary_rows={enums_result['dictionary_rows']} "
                f"enum_rst={enums_result['output_rst']}"
            )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
