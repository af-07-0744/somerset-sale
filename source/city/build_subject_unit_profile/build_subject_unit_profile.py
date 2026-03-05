"""Workflow step entry for subject unit profile generation."""

from .city_data_inventory import main as city_data_inventory_main


def main() -> int:
    return int(city_data_inventory_main())
