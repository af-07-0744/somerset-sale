"""Workflow step entry for building/unit inventory outputs."""

from .city_data_to_rst import main as city_data_to_rst_main


def main() -> int:
    return int(city_data_to_rst_main())
