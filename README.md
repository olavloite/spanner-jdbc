https://travis-ci.org/olavloite/spanner-jdbc.svg?branch=master

# spanner-jdbc
JDBC Driver for Google Cloud Spanner

An open source JDBC Driver for Google Cloud Spanner, the horizontally scalable, globally consistent, relational database service from Google. The JDBC Driver that is supplied by Google is quite limited, as it does not allow any inserts, updates or deletes, nor does it allow DDL-statements.

This driver supports a number of unsupported features of the official JDBC driver:
* DML-statements (INSERT, UPDATE, DELETE)
* DDL-statements (CREATE TABLE, ALTER TABLE, CREATE INDEX, DROP TABLE, ...)
* Transactions (both read/write and read-only)

The driver ofcourse also supports normal SELECT-statements, including parameters.

Releases are available on Maven Central. Current release is version 0.12.

<div class="highlight highlight-text-xml"><pre>
&lt;<span class="pl-ent">dependency</span>&gt;
 	&lt;<span class="pl-ent">groupId</span>&gt;nl.topicus&lt;/<span class="pl-ent">groupId</span>&gt;
    	&lt;<span class="pl-ent">artifactId</span>&gt;spanner-jdbc&lt;/<span class="pl-ent">artifactId</span>&gt;
    	&lt;<span class="pl-ent">version</span>&gt;0.12&lt;/<span class="pl-ent">version</span>&gt;
&lt;/<span class="pl-ent">dependency</span>&gt;
</pre></div>

There is also a 'thick-jar'-version available for use with third-party tools such as SQuirreL, DbVisualizer or Safe FME. This jar contains all the necessary dependencies for the driver. The thick-jar version can be found here: https://github.com/olavloite/spanner-jdbc/releases


This driver does allow DML operations, although also limited because of the underlying limitations of Google Cloud Spanner. All data manipulation operations are limited to operations that operate on one record. This means that:
* Inserts can only insert one row at a time
* Updates and deletes must include a where-clause specifying the primary key (and nothing else). E.g. 'WHERE ID=?'.

It does of course allow several updates to be bundled together in one transaction.

The driver is designed to work with applications using JPA/Hibernate. See https://github.com/olavloite/spanner-hibernate for a Hibernate Dialect implementation for Google Cloud Spanner that works together with this JDBC Driver.

A simple example project using Spring Boot + JPA + Hibernate + this JDBC Driver can be found here: https://github.com/olavloite/spanner-jpa-example

Example usage:

****
spring.datasource.driver-class-name=nl.topicus.jdbc.CloudSpannerDriver

spring.datasource.url=jdbc:cloudspanner://localhost;Project=projectId;Instance=instanceId;Database=databaseName;SimulateProductName=PostgreSQL;PvtKeyPath=key_file

****


The last two properties (SimulateProductName and PvtKeyPath) are optional.
All properties can also be supplied in a Properties object instead of in the URL.

You either need to
* Create an environment variable GOOGLE_APPLICATION_CREDENTIALS that points to a credentials file for a Google Cloud Spanner project.
* OR Supply the parameter PvtKeyPath that points to a file containing the credentials to use.

The server name (in the example above: localhost) is ignored by the driver, but as it is a mandatory part of a JDBC URL it needs to be specified.
The property 'SimulateProductName' indicates what database name should be returned by the method DatabaseMetaData.getDatabaseProductName(). This can be used in combination with for example Spring Batch. Spring Batch automatically generates a schema for batch jobs, parameters etc., but does so only if it recognizes the underlying database. Supplying PostgreSQL as a value for this parameter, ensures the correct schema generation.


Credits
This application uses Open Source components. You can find the source code of their open source projects along with license information below.

A special thanks to Tobias for his great JSqlParser library.
Project: JSqlParser https://github.com/JSQLParser/JSqlParser 
Copyright (C) 2004 - 2017 JSQLParser Tobias
