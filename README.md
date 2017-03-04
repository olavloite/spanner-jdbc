# spanner-jdbc
JDBC Driver for Google Cloud Spanner

An open source JDBC Driver for Google Cloud Spanner, the horizontally scalable, globally consistent, relational database service from Google. The JDBC Driver that is supplied by Google is seriously limited, as it does not allow any inserts, updates or deletes. This driver does allow these operations, although also limited. All data manipulation operations are limited to operations that operate on one record. This means that:
* Inserts can only insert one row at a time
* Updates and deletes must include a where-clause specifying the primary key.
