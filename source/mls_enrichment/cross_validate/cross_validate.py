"""Workflow step entry for cross-source field validation."""

from .audit_realtor_accuracy import main as audit_realtor_accuracy_main


def main() -> int:
    return int(audit_realtor_accuracy_main())
