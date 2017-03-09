# spanner-jdbc
JDBC Driver for Google Cloud Spanner

An open source JDBC Driver for Google Cloud Spanner, the horizontally scalable, globally consistent, relational database service from Google. The JDBC Driver that is supplied by Google is seriously limited, as it does not allow any inserts, updates or deletes, nor does it allow DDL-statements. This driver does allow these operations, although also limited because of the underlying limitations of Google Cloud Spanner. All data manipulation operations are limited to operations that operate on one record. This means that:
* Inserts can only insert one row at a time
* Updates and deletes must include a where-clause specifying the primary key.

The driver is designed to work with applications using JPA/Hibernate. See https://github.com/olavloite/spanner-hibernate for a Hibernate Dialect implementation for Google Cloud Spanner that works together with this JDBC Driver.
The driver currently ignores transaction statements (commit/rollback) and effectively runs in autocommit mode.

The driver also supports DDL-statements, although the DDL syntax of Google Cloud Spanner is quite limited in comparison to most relational databases.

Example usage:
spring.datasource.driver-class-name=nl.topicus.jdbc.CloudSpannerDriver
spring.datasource.url=jdbc:cloudspanner://localhost;Project=projectId;Instance=instanceId;Database=databaseName;SimulateProductName=PostgreSQL;PvtKeyPath=key_file

The last two properties (SimulateProductName and PvtKeyPath) are optional.

You either need to
	1. Create an environment variable GOOGLE_APPLICATION_CREDENTIALS that points to a credentials file for a Google Cloud 	Spanner project.
	2. OR Supply the parameter PvtKeyPath that points to a file containing the credentials to use.

The server name (in the example above: localhost) is ignored by the driver, but as it is a mandatory part of a JDBC URL it needs to be specified.
The property 'SimulateProductName' indicates what database name should be returned by the method DatabaseMetaData.getDatabaseProductName().

Releases are available on Maven Central. Current release is version 0.1.

<dependency>
	<groupId>nl.topicus</groupId>
	<artifactId>spanner-jdbc</artifactId>
	<version>0.1</version>
</dependency>


TODO:
* Implement transactions


Credits
This application uses Open Source components. You can find the source code of their open source projects along with license information below.

A special thanks to Tobias for his great JSqlParser library.
Project: JSqlParser https://github.com/JSQLParser/JSqlParser 
Copyright (C) 2004 - 2017 JSQLParser Tobias

