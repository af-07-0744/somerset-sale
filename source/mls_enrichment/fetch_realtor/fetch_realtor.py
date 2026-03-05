"""Workflow step entry for realtor.ca data pull."""

from .extract_realtor_listing import main as extract_realtor_listing_main


def main() -> int:
    return int(extract_realtor_listing_main())
