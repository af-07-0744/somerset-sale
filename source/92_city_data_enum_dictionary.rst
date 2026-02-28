City Data Enumeration Dictionary
================================

Enumerations and code mappings derived from the fetched city-data CSV.

- Generated at: ``2026-02-28 07:52 UTC``
- Input rows: ``838``
- Fields documented: ``7``
- Last-run fetch/request metadata: :doc:`93_city_data_fetch_metadata`

.. contents::
   :local:
   :depth: 2

``How Resolved*`` footnote: [#how-resolved]_.

``comm_code``
-------------

- Meaning: Community code from the assessment record.
- Present: ``838/838`` (``100.00%``)
- Distinct non-empty values: ``1``

.. csv-table:: Enumerations for ``comm_code``
   :file: city_data/_tables/enums/comm_code.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

``land_size_ac``
----------------

- Meaning: Land size in acres from the assessment record.
- Present: ``838/838`` (``100.00%``)
- Distinct non-empty values: ``11``

.. csv-table:: Enumerations for ``land_size_ac``
   :file: city_data/_tables/enums/land_size_ac.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

``land_size_sf``
----------------

- Meaning: Land size in square feet from the assessment record.
- Present: ``838/838`` (``100.00%``)
- Distinct non-empty values: ``11``

.. csv-table:: Enumerations for ``land_size_sf``
   :file: city_data/_tables/enums/land_size_sf.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

``land_size_sm``
----------------

- Meaning: Land size in square metres from the assessment record.
- Present: ``838/838`` (``100.00%``)
- Distinct non-empty values: ``11``

.. csv-table:: Enumerations for ``land_size_sm``
   :file: city_data/_tables/enums/land_size_sm.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

``land_use_designation``
------------------------

- Meaning: Land-use designation code from the assessment record.
- Present: ``838/838`` (``100.00%``)
- Distinct non-empty values: ``3``

.. csv-table:: Enumerations for ``land_use_designation``
   :file: city_data/_tables/enums/land_use_designation.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

``sub_property_use``
--------------------

- Meaning: Sub-property-use code from the assessment record.
- Present: ``838/838`` (``100.00%``)
- Distinct non-empty values: ``5``

.. csv-table:: Enumerations for ``sub_property_use``
   :file: city_data/_tables/enums/sub_property_use.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

``year_of_construction``
------------------------

- Meaning: Year of construction from the assessment record.
- Present: ``835/838`` (``99.64%``)
- Distinct non-empty values: ``3``

.. csv-table:: Enumerations for ``year_of_construction``
   :file: city_data/_tables/enums/year_of_construction.csv
   :header-rows: 1
   :class: enum-table-angled
   :widths: auto

Downloads
---------

- Input Flat CSV: :download:`open_calgary_somervale_raw_rows_flat.csv <city_data/_downloads/enums/data__open_calgary_somervale_raw_rows_flat.csv>`
- Enumeration Dictionary CSV: :download:`open_calgary_street_requested_field_dictionary.csv <city_data/_downloads/enums/data__open_calgary_street_requested_field_dictionary.csv>`
- Fetch Run Metadata JSON: :download:`open_calgary_somervale_raw_rows_meta.json <city_data/_downloads/enums/data__open_calgary_somervale_raw_rows_meta.json>`
- Enum Table CSV (comm_code): :download:`comm_code.csv <city_data/_downloads/enums/enums__comm_code.csv>`
- Enum Table CSV (land_size_ac): :download:`land_size_ac.csv <city_data/_downloads/enums/enums__land_size_ac.csv>`
- Enum Table CSV (land_size_sf): :download:`land_size_sf.csv <city_data/_downloads/enums/enums__land_size_sf.csv>`
- Enum Table CSV (land_size_sm): :download:`land_size_sm.csv <city_data/_downloads/enums/enums__land_size_sm.csv>`
- Enum Table CSV (land_use_designation): :download:`land_use_designation.csv <city_data/_downloads/enums/enums__land_use_designation.csv>`
- Enum Table CSV (sub_property_use): :download:`sub_property_use.csv <city_data/_downloads/enums/enums__sub_property_use.csv>`
- Enum Table CSV (year_of_construction): :download:`year_of_construction.csv <city_data/_downloads/enums/enums__year_of_construction.csv>`

Explanatory Data Dictionary
---------------------------

- ``Count``: number of rows containing the value.
- ``%``: percentage of all rows containing the value (formerly ``value_pct_of_rows``).
- ``% Present``: percentage among non-blank rows for the field (formerly ``value_pct_of_present``).
- ``How Resolved*``: resolver tag for how ``Meaning`` was obtained; see [#how-resolved]_.

Resolver Tags
^^^^^^^^^^^^^

- ``known_map``: meaning comes from explicit code mapping hardcoded in the script.
- ``companion:<field>``: meaning is copied from a paired descriptor field in the same source row.
  Example: ``comm_code=SOM`` resolves as ``companion:comm_name`` because ``comm_name=SOMERSET``.
- ``derived``: meaning generated from numeric context (for example year or land-size units).
- ``missing``: source value was blank.

.. [#how-resolved] ``How Resolved*`` indicates the rule used to populate ``Meaning`` for each value.
   It does not indicate confidence; it indicates transformation provenance.
