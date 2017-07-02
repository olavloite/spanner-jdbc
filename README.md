# spanner-jdbc
JDBC Driver for Google Cloud Spanner

An open source JDBC Driver for Google Cloud Spanner, the horizontally scalable, globally consistent, relational database service from Google. The JDBC Driver that is supplied by Google is quite limited, as it does not allow any inserts, updates or deletes, nor does it allow DDL-statements.

This driver supports a number of unsupported features of the official JDBC driver:
* DML-statements (INSERT, UPDATE, DELETE)
* DDL-statements (CREATE TABLE, ALTER TABLE, CREATE INDEX, DROP TABLE, ...)
* Transactions

The driver ofcourse also supports normal SELECT-statements, including parameters.

This driver does allow DML operations, although also limited because of the underlying limitations of Google Cloud Spanner. All data manipulation operations are limited to operations that operate on one record. This means that:
* Inserts can only insert one row at a time
* Updates and deletes must include a where-clause specifying the primary key (and nothing else).

It does of course allow several updates to be bundled together in one transaction.

The driver is designed to work with applications using JPA/Hibernate. See https://github.com/olavloite/spanner-hibernate for a Hibernate Dialect implementation for Google Cloud Spanner that works together with this JDBC Driver.

A simple example project using Spring Boot + JPA + Hibernate + this JDBC Driver can be found here: https://github.com/olavloite/spanner-jpa-example

Example usage:

****
spring.datasource.driver-class-name=nl.topicus.jdbc.CloudSpannerDriver

spring.datasource.url=jdbc:cloudspanner://localhost;Project=projectId;Instance=instanceId;Database=databaseName;SimulateProductName=PostgreSQL;PvtKeyPath=key_file

****


The last two properties (SimulateProductName and PvtKeyPath) are optional.

You either need to
* Create an environment variable GOOGLE_APPLICATION_CREDENTIALS that points to a credentials file for a Google Cloud Spanner project.
* OR Supply the parameter PvtKeyPath that points to a file containing the credentials to use.

The server name (in the example above: localhost) is ignored by the driver, but as it is a mandatory part of a JDBC URL it needs to be specified.
The property 'SimulateProductName' indicates what database name should be returned by the method DatabaseMetaData.getDatabaseProductName(). This can be used in combination with for example Spring Batch. Spring Batch automatically generates a schema for batch jobs, parameters etc., but does so only if it recognizes the underlying database. Supplying PostgreSQL as a value for this parameter, ensures the correct schema generation.

Releases are available on Maven Central. Current release is version 0.7.

<div class="highlight highlight-text-xml"><pre>
	&lt;<span class="pl-ent">dependency</span>&gt;
    		&lt;<span class="pl-ent">groupId</span>&gt;nl.topicus&lt;/<span class="pl-ent">groupId</span>&gt;
    		&lt;<span class="pl-ent">artifactId</span>&gt;spanner-jdbc&lt;/<span class="pl-ent">artifactId</span>&gt;
    		&lt;<span class="pl-ent">version</span>&gt;0.7&lt;/<span class="pl-ent">version</span>&gt;
	&lt;/<span class="pl-ent">dependency</span>&gt;
</pre></div>

There is also a 'thick-jar'-version available for use with tools such as SQuirreL, DbVisualizer or Safe FME. This jar contains all the necessary dependencies for the driver. The thick-jar version can be found here: https://github.com/olavloite/spanner-jdbc/releases


Credits
This application uses Open Source components. You can find the source code of their open source projects along with license information below.

A special thanks to Tobias for his great JSqlParser library.
Project: JSqlParser https://github.com/JSQLParser/JSqlParser 
Copyright (C) 2004 - 2017 JSQLParser Tobias

<form action="https://www.paypal.com/cgi-bin/webscr" method="post" target="_top">
<input type="hidden" name="cmd" value="_s-xclick">
<input type="hidden" name="encrypted" value="-----BEGIN PKCS7-----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-----END PKCS7-----
">
<input type="image" src="https://www.paypalobjects.com/en_US/GB/i/btn/btn_donateCC_LG.gif" border="0" name="submit" alt="PayPal – The safer, easier way to pay online!">
<img alt="" border="0" src="https://www.paypalobjects.com/de_DE/i/scr/pixel.gif" width="1" height="1">
</form>
