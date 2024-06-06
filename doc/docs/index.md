# Introduction
The Open Health Stack's Analytics components provide a scalable and flexible collection of tools to transform complex [HL7 FHIR](https://www.hl7.org/fhir/overview.html) data into formats for running _analytics_ workloads and building downstream applications. 

Using OHS, developers can use familiar languages, packages and tools to build analytics solutions for different use cases: from generating reports and powering dashboards to exploratory data science and machine learning.

![FHIR Data Pipes Image](images/v3_FHIR_Data_Pipes.png)

## Key features

*	Transform FHIR resources to _"near lossless"_ Parquet on FHIR representation based on the ["Simplified SQL Projection of FHIR Resources"](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md) schema

* 	Apache Beam based ETL pipeline to continuously transform FHIR data for use in downstream applications. With support for local, on-prem or cloud based runners 

* Flexible deployment modes to meet the needs of different projects and teams from simple single machine to multi-worker horizontally scalable distributed environments. 

* 	Seamless support for different target databases including traditional RDBMS (such as [PostgreSQL](https://www.postgresql.org/)) or OLAP Database Engines that can load Parquet files (such as [SparkSQL](https://spark.apache.org/sql/) or [DuckDB](https://duckdb.org/)) 

*	Define views in SQL or as [ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html) resources to create flattened tables to make it easier to query data using common languages like SQL or python

## Use cases

*   The **primary use case** for FHIR Data Pipes is to enable continuous transformation of FHIR Data into analytics friendly representations to make it easier for developers to: build dashboards, generate reports, do data science and machine learning

*   A **secondary use case** is for piping FHIR data from a FHIR sources to another FHIR server e.g. for integration into a central FHIR repository