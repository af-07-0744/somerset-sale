Comparables
===========

Selection Rules
---------------

- Prefer sold comps over active listings.
- Prioritize same building, then closest substitute buildings.
- Keep comp dates recent unless market evidence requires older sales.
- Reject comps without complete source provenance.

Comparable Grid
---------------

The table below is sourced from ``data/comps_clean.csv``.

.. csv-table:: Comparable Summary
   :file: ../data/comps_clean.csv
   :header-rows: 1

Interpretation Notes
--------------------

- ``source_ids`` is a semicolon-delimited list mapping each comp to evidence rows
  in ``data/source_registry.csv``.
- Any comp without ``source_ids`` must be removed before final issue.
