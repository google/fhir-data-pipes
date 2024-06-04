# Components

## FHIR Data Pipes 
FHIR Data Pipes is built on technology **designed for horizontal scalability** and has multiple deployment options depending on your use case and requirements.

### ETL Pipelines
The ETL Pipeline is a Java JAR which moves and transforms data from a FHIR server to Apache Parquet files for data analysis or another FHIR server for data integration. 

Pipelines is built using Apache Beam and can run on a single machine or cluster.

**Extraction**

FHIR Data Pipes is designed to work with FHIR source data in various forms: 

* Valid FHIR Server API (e.g. HAPI FHIR)
* FHIR facade implementations (e.g. OpenMRSv3)
* Bulk Export API data
* ndjson representation

**Transformation**

FHIR Resources are transformed into a "Parquet on FHIR" format:

* Uses a forked version of [Bunsen library](https://github.com/google/fhir-data-pipes/tree/master/bunsen) to transform from FHIR (_current support for STU3, R4_) to the SQL-on-FHIR-v1 schema. 
* Configurable support for FHIR profiles and extensions
* In-pipeline 'flattening' of FHIR data using [ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html) resources

**Loading**

FHIR Data Pipes supports different SQL Data Warehouse options depending on the needs of the project. These include:

* Loading Parquet into an OLAP data warehouse such as DuckDB, Clickhouse or distributed Query Engine such as SparkSQL, PrestoDB, TrinoDB
* Traditional Relational data warehouse such as PostgreSQL (when using the FHIR ViewDefinitions resources to generate materialized views)

### Pipelines Controller
A user-interface wrapper for the FHIR Data Pipes Pipelines, integrating "full", "incremental", and "merger" pipelines together. 

* The Pipelines Controller is built on top of pipelines and shares many of the same settings
* Using the controller module you can schedule periodic incremental updates or use a web control panel to start the pipeline manually

### Web Control Panel
The web control panel is a basic spring application provided to make interacting with the pipeline controller easier. 

_It **is not** designed to be a full production ready “web admin” panel_.

The web control panel has the following features:

*   Initiate full and incremental pipeline runs
*   Monitor errors when running pipelines
*   Recreate view tables
*   View configuration settings
*   Access sample jupyter notebooks and ViewDefinition editor

![Web Control Panel](images/pipelines_control_panel.png)

## Querying FHIR Data
Once the FHIR data has been transformed via the ETL Pipelines, the resulting schema is available for querying using a JDBC interface. 

Two common approaches for this include:

1. SQL-on-FHIR queries
2. Flattening data

### SQL-on-FHIR queries
When working with the SQL-on-FHIR schema, one challenge is handling the many nested or repeated fields (such as condeable concepts). 

This depends on the different nuances of SQL dialects. For example:

*   _Spark SQL_: `LATERL VIEW explode()`
*   _BigQuery:_ `Simple JOIN`
*   _JSON:_ `CROSS JOIN LATERAL jsonb_array_elements()`

Example of an SQL-on-FHIR query using the EXPLODE and LATERAL VIEW approach in Spark SQL

``` sql
SELECT
  O.id AS obs_id, O.subject.PatientId AS patient_id,
  O.status AS status
FROM Observation AS O LATERAL VIEW explode(code.coding)
  AS OCC LATERAL VIEW
  explode(O.value.codeableConcept.coding) AS OVCC
WHERE OCC.`system`=
    "https://openconceptlab.org/orgs/CIEL/sources/CIEL"
  AND OCC.code="1255"
  AND OVCC.`system`=
    "https://openconceptlab.org/orgs/CIEL/sources/CIEL"
  AND OVCC.code="1256"
  AND YEAR(O.effective.dateTime) = 2010;
```

### Flattening data
A common pattern to address this is to flatten the data into a set of views of tables which can then be queried using simpler SQL statements. 

To make it easier to query data, FHIR Data Pipes provides two approaches for flattening the FHIR Resources into virtual or materialized views

1.  SQL queries to generate virtual views (outside the pipeline)
2.  Apply FHIR ViewDefinition resources to generated materialized views (within the pipeline)

For both of these approaches, **"predefined views”** (SQL-on-FHIR queries or ViewDefintion resources) for common resources are provided. These can be modified or extended.

You can read more about the different approaches in the advanced guides section.

## ViewDefinition editor
The ViewDefinition editor provides a way to quickly evaluate ViewDefinition resources against sample FHIR data. You access it as part of the Web Control Panel, selecting the "Views" navigation item in the top right corner.

Using the ViewDefinition editor you can:

* Provide an input ViewDefinition (left)
* Apply it to a sample input FHIR Resource (right pane)
* View the results in the generated table (top)

![FHIR Data Pipes Image](images/view_definition_editor.png)
