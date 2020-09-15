### Status
[![Build Status](https://travis-ci.org/olavloite/spanner-jdbc.svg?branch=master)](https://travis-ci.org/olavloite/spanner-jdbc)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=nl.topicus%3Aspanner-jdbc&metric=alert_status)](https://sonarcloud.io/dashboard/index/nl.topicus%3Aspanner-jdbc)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=nl.topicus%3Aspanner-jdbc&metric=coverage)](https://sonarcloud.io/dashboard/index/nl.topicus%3Aspanner-jdbc)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=nl.topicus%3Aspanner-jdbc&metric=reliability_rating)](https://sonarcloud.io/dashboard/index/nl.topicus%3Aspanner-jdbc)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=nl.topicus%3Aspanner-jdbc&metric=security_rating)](https://sonarcloud.io/dashboard/index/nl.topicus%3Aspanner-jdbc)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=nl.topicus%3Aspanner-jdbc&metric=sqale_rating)](https://sonarcloud.io/dashboard/index/nl.topicus%3Aspanner-jdbc)

# DEPRECATED

**Note: There is now a Google backed open source JDBC driver for Google Cloud Spanner.**
It is recommended that you use that driver. It can be found here: https://github.com/googleapis/java-spanner-jdbc

This community driver will continue to exist in its current form. It will however not implement any new features that Cloud Spanner might add in the future.

## spanner-jdbc
Community Open Source JDBC Driver for Google Cloud Spanner

Include the following if you want the thick jar version that includes all (shaded) dependencies. This is the recommended version unless you know that the transitive dependencies of the small jar will not conflict with the rest of your project.

<div class="highlight highlight-text-xml"><pre>
&lt;<span class="pl-ent">dependency</span>&gt;
 	&lt;<span class="pl-ent">groupId</span>&gt;nl.topicus&lt;/<span class="pl-ent">groupId</span>&gt;
    	&lt;<span class="pl-ent">artifactId</span>&gt;spanner-jdbc&lt;/<span class="pl-ent">artifactId</span>&gt;
    	&lt;<span class="pl-ent">version</span>&gt;1.1.6&lt;/<span class="pl-ent">version</span>&gt;
&lt;/<span class="pl-ent">dependency</span>&gt;
</pre></div>

Downloads for both the current and older versions can be found here: https://github.com/olavloite/spanner-jdbc/releases

Building your own version can be done using:

`mvn install -DskipITs`

(See the section [Building](#building) for more information on this)

## JPA and Hibernate
This driver can be used with JPA and Hibernate. It is however recommended to use the officially supported driver at https://github.com/googleapis/java-spanner-jdbc in combination with the [officially supported Hibernate dialect](https://github.com/GoogleCloudPlatform/google-cloud-spanner-hibernate).

## Building
The driver is by default a 'thick' jar that contains all the dependencies it needs. The dependencies are shaded to avoid any conflicts with dependencies you might use in your own project. Shading and adding the dependencies to the jar is linked to the `post-integration-test` phase of Maven. This means that you could build both a thin and a thick jar to use with your project, but please be aware that only the thick jar version is supported. If you decide to use the thin jar you need to supply the dependencies yourself.

### Building a thick jar (default)

`mvn install -DskipITs`

Skipping the integration tests while building is necessary as these will try to connect to a default Cloud Spanner instance (or Cloud Spanner emulator) to run the tests on. The key file for authenticating on these default instances are not included in the source code.

### Building a thin jar (not recommended)

`mvn package`

This will give you a jar containing only the compiled source of the JDBC driver without the necessary dependencies. You will have to supply these yourself. This is not the recommended way of using the driver, unless you know what you are doing.


### Credits
This application uses Open Source components. You can find the source code of their open source projects along with license information below.

A special thanks to Tobias for his great JSqlParser library.
Project: JSqlParser https://github.com/JSQLParser/JSqlParser 
Copyright (C) 2004 - 2017 JSQLParser Tobias
