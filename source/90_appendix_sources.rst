Appendix: Source Provenance
===========================

Source Registry
---------------

.. csv-table:: Source Registry
   :file: ../data/source_registry.csv
   :header-rows: 1

Comparable-to-Source Mapping
----------------------------

.. csv-table:: Comparable Mapping
   :file: ../data/comps_clean.csv
   :header-rows: 1

Required Source Fields
----------------------

- ``source_id``: unique ID used in memo citations (example: ``S-0001``)
- ``comp_id``: comp row ID (example: ``C-0003``)
- ``source_type``: ``MLS``, ``web``, ``agent_email``, or ``other``
- ``mls_number``: MLS listing ID when available
- ``url``: canonical URL used to capture the claim
- ``captured_at``: ISO timestamp of capture
- ``captured_by``: who collected the data
- ``file_path``: local evidence artifact path
- ``file_sha256``: integrity hash for saved artifact
- ``claims_supported``: semicolon-delimited list of facts this source supports

Validation Rule
---------------

Run ``poetry run python -m codex_sale_docs.provenance`` before issuing the memo.
