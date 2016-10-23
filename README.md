# hades
SAP HANA dependencies utilities

SAP/HANA's OBJECT_DEPENDENCIES system view (OBJECT_DEPENDENCIES ) allows basic dependency analysis of information views (analytical views, attribute views and calculation views.)

Unfortunately, OBJECT_DEPENDENCIES does not provide any information at the column level.

We at just-bi.nl are interested in being able to query the structure of information_views, for example, to report on column-level dependencies. This project is a bunch of scripts and utility objects (mosty, stored procedures) that allow you to do just that.