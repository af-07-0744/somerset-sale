"""Workflow step entry for same-unit cross-building matching."""

from .fetch_open_calgary import main as fetch_open_calgary_main


def main() -> int:
    return int(fetch_open_calgary_main())
