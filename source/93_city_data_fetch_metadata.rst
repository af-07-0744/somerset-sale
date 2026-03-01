City Data Fetch Metadata
========================

Last fetch metadata and generated inventory statistics for City of Calgary data.

.. contents::
   :local:
   :depth: 2

Request-to-File Communication Diagram
-------------------------------------

- Requests in this run: ``1`` subject-lookup + ``1`` street-fetch.
- Subject-lookup labels from request index: ``209 % 3000 % SOMERVALE``
- Street-fetch labels from request index: ``SOMERVALE CO SW``
- Numbering indicates order; arrows show only query-to-query and query/file-to-file data flow.

.. uml::

   @startuml
   top to bottom direction
   allowmixing
   hide circle
   skinparam linetype polyline
   skinparam shadowing false
   skinparam nodesep 20
   skinparam ranksep 30

   class "**209 % 3000 % SOMERVALE**" as query_1 <<json>> {
     + address: text
     ..
     1 row
     ==
     Subject lookup query
   }
   class "**SOMERVALE CO SW**" as query_2 <<json>> {
     + address: text
     + roll_number: integer[9]
     + unique_key: integer[21]
     + cpid: integer[8]
     + comm_code: { "SOM" }
     + comm_name: { "SOMERSET" }
     + assessed_value: price
     + re_assessed_value: price
     + nr_assessed_value: price
     + fl_assessed_value: price
     + roll_year: integer[4]
     + property_type: { "LI" | "LO" }
     + assessment_class: { "NR" | "RE" }
     + assessment_class_description: { "Non-Residential" | "Residential" }
     + land_use_designation: { "M-C2" | "S-SPR" | "S-UN" }
     + sub_property_use: { "A004" | "A005" | "A006" | "R201" | "X057" }
     + year_of_construction: 2001 .. 2003
     + land_size_sm: 4530 .. 15100
     + land_size_sf: 48762 .. 162540
     + land_size_ac: 1.12 .. 3.73
     + mod_date: datetime[ISO-8601 UTC]
     ..
     838 raw / 838 unique
     ==
     Street fetch query
   }
   file "data/open_calgary_somervale_raw_rows.json" as raw_json
   file "data/open_calgary_somervale_raw_rows_flat.csv" as flat_csv
   file "data/open_calgary_somervale_raw_\nfield_profile.csv" as field_profile
   file "data/open_calgary_somervale_inventory_\nsummary.json" as inventory_summary
   file "data/open_calgary_street_requested_\nfield_dictionary.csv" as enum_dictionary
   file "source/92_city_data_enum_dictionary.rst" as enum_rst
   file "source/91_city_data_index.rst\nsource/city_data/building_*.rst" as city_pages
   file "source/93_city_data_fetch_metadata.rst" as metadata_rst

   query_1 --> query_2 : 1) street_portion

   query_2 --> raw_json
   raw_json -down-> flat_csv : 3) flattened rows
   flat_csv -down-> field_profile : 4) field profile

   flat_csv -down-> inventory_summary : 5) inventory output
   flat_csv -right-> enum_dictionary : 6) enum dictionary
   enum_dictionary -down-> enum_rst : 7) dictionary source

   flat_csv -right-> city_pages : 8) page rows
   raw_json -right-> city_pages : 9) provenance

   inventory_summary -down-> metadata_rst : 10) inventory input
   field_profile -[hidden]right-> inventory_summary
   inventory_summary -[hidden]right-> enum_dictionary
   enum_dictionary -[hidden]right-> city_pages
   @enduml

Fetch Summary
-------------

.. csv-table:: Run Summary
   :file: city_data/_tables/meta/run_summary.csv
   :header-rows: 1
   :widths: auto

Where Clauses
-------------

.. csv-table:: Request WHERE Clauses
   :file: city_data/_tables/meta/where_clauses.csv
   :header-rows: 1
   :widths: auto

Subject Lookup Queries
----------------------

.. csv-table:: Subject Lookup Query Attempts
   :file: city_data/_tables/meta/subject_lookup_queries.csv
   :header-rows: 1
   :widths: auto

Request Index
-------------

.. csv-table:: Request Index
   :file: city_data/_tables/meta/request_index.csv
   :header-rows: 1
   :widths: auto

Query URLs
----------

.. csv-table:: Query URLs Used
   :file: city_data/_tables/meta/query_urls.csv
   :header-rows: 1
   :widths: auto

Inventory Summary
-----------------

- Inventory summary JSON: ``data/open_calgary_somervale_inventory_summary.json``

.. csv-table:: Inventory Bucket Summary
   :file: city_data/_tables/meta/inventory_summary.csv
   :header-rows: 1
   :widths: auto
