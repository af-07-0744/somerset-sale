"""Workflow step entry for generalized cross-building matches."""

from .infer_open_calgary_units import main as infer_open_calgary_units_main


def main() -> int:
    return int(infer_open_calgary_units_main())
