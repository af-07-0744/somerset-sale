"""Workflow step entry for normalization/dedupe support."""

from .osm_address_lookup import main as osm_address_lookup_main


def main() -> int:
    return int(osm_address_lookup_main())
