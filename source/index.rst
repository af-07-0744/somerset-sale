Pricing Evidence Pack
=====================

This document set is designed to support a high but defensible sale price for
``3000 Somervale Court SW #209, Calgary AB`` using traceable evidence.

Workflow Overview
-----------------

.. uml::

   top to bottom direction
   hide circle
   skinparam shadowing false
   skinparam linetype polyline
   skinparam nodesep 20
   skinparam ranksep 30

   rectangle "Step 1 Get City Data" as Step1
   rectangle "Step 2 Infer Assessment Based Comparables" as Step2
   rectangle "Step 3 Get MLS and Predictive Data\nSubject and Comps" as Step3
   rectangle "Step 4 Determine Fair Market Value\nand Justification" as Step4

   package "Assessment Inference" as Assess {
     rectangle "Compare same unit number across buildings\nwhen present" as A1
     rectangle "Measure floor ladder effect\n109 209 309 409" as A2
     rectangle "Find same floor units with same or similar\nassessed value" as A3
     rectangle "Extend matching logic to other buildings" as A4
     rectangle "Fit provisional value formula\nweights and multipliers" as A5
     A1 --> A2
     A2 --> A3
     A3 --> A4
     A4 --> A5
   }

   package "MLS + Predictive Enrichment" as Enrich {
     rectangle "Collect from realtor.ca" as C1
     rectangle "Collect from zillow.com" as C2
     rectangle "Collect from zolo.ca" as C3
     rectangle "Test beds baths sqft floor\nagainst inferred multipliers" as C4
     rectangle "Cross source validation\nand conflict resolution" as C5
     C1 --> C4
     C2 --> C4
     C3 --> C4
     C4 --> C5
   }

   rectangle "Output FMV range and point estimate\nwith clear evidence based rationale" as Output

   Step1 --> Step2
   Step2 --> Step3
   Step3 --> Step4
   Step2 --> A1
   A5 --> Step3
   Step3 --> C1
   C5 --> Step4
   Step4 --> Output
   @enduml

.. toctree::
   :maxdepth: 2
   :caption: Memo

   01_executive_summary
   02_property_profile
   03_comparables
   03b_renter_comps_generated
   03c_renter_comps_stretch_generated
   04_adjustments
   05_price_conclusion
   06_terms_and_deadline
   91_city_data_index
   92_city_data_enum_dictionary
   93_city_data_fetch_metadata
   90_appendix_sources
