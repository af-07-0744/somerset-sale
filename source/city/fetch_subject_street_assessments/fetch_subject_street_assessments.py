"""Workflow step entry for full subject-street assessment fetch."""

from .fetch_city_data import main as fetch_city_data_main


def main() -> int:
    return int(fetch_city_data_main())
