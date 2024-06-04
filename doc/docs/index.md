# Introduction

The Open Health Stack's [FHIR Data Pipes](https://https://github.com/google/fhir-data-pipes) provides a collection of tools to transform complex FHIR (Fast Healthcare Interoperability Resources) data into formats for running _analytics_ workloads and building services. 

Using OHS, developers can use familiar languages (such as python or ANSI SQL), packages and tools to build analytics solutions for different use cases: from generating reports and powering dashboards to exploratory data science and machine learning.

![FHIR Data Pipes Image](images/v2_FHIR_Data_Pipes.png)

## Key features

*	Transform FHIR JSON Resources (STU3 and R4) to (near) _"lossless"_ Parquet on FHIR representation based on the [SQL-on-FHIR-v1 schema](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md* )

* 	**Horizontally scalable** Apache Beam based ETL pipeline to continuously transform FHIR data for use in downstream applications. Support for local, on-prem or cloud runners and deployment options

* 	Seamless support for different Data Warehouses from traditional RDBMS (such as [PostgreSQL](https://www.postgresql.org/)), OLAP Database Engines (such as [duckdb](https://duckdb.org/)) to **Distributed query engines** (such as [SparkSQL](https://spark.apache.org/sql/), [Trino](https://trino.io/) or [PrestoDB](https://prestodb.io/))

*	Define views in ANSI SQL or as [SQL-on-FHIR-v2](/v2.html) ViewDefinition Resources to create flattened tables to power different analytics solutions

## Use cases

*   The **primary use case** for FHIR Data Pipes is to enable continuous transformation of FHIR Data into analytics friendly representations.

*   A _secondary use case_ is for piping FHIR data from a FHIR sources to another FHIR server e.g. for integration into a central FHIR repository