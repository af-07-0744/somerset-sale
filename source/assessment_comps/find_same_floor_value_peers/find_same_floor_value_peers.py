"""Workflow step entry for same-floor value peer extraction."""

from .prepare_renter_comps import main as prepare_renter_comps_main


def main() -> int:
    return int(prepare_renter_comps_main())
