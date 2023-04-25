The
[pipelines](https://github.com/google/fhir-data-pipes/blob/master/pipelines/)
directory contains code to transform data from a FHIR server to either
[Apache Parquet files](https://parquet.apache.org) for analysis or another FHIR
store for data integration.

There are two options for reading the source FHIR server (input):

- _FHIR-Search_: This mode uses FHIR Search APIs to select resources to copy,
  retrieves them as FHIR resources, and transfers the data via FHIR APIs or
  Parquet files. This mode should work with any FHIR server.
- _JDBC_: This mode uses the
  [Java Database Connectivity (JDBC) API](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/)
  to read FHIR resources directly from the database of a
  [HAPI FHIR server configured to use a PostgreSQL database](https://github.com/hapifhir/hapi-fhir-jpaserver-starter#postgresql-configuration)
  or an [OpenMRS](https://openmrs.org/) instance using MySQL. Note: JDBC support
  beyond HAPI FHIR and OpenMRS is not currently planned. Our long-term approach
  for a generic high-throughput alternative is to use the
  [FHIR Bulk Export API](https://build.fhir.org/ig/HL7/bulk-data/export.html).

There are two options for transforming the data (output):

- _Parquet_: Outputs the FHIR resources as Parquet files, using the
  [SQL-on-FHIR schema](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md).
- _FHIR_: Copies the FHIR resources to another FHIR server using FHIR APIs.

## Setup

1.  [Clone](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository)
    the [FHIR Data Pipes](https://github.com/google/fhir-data-pipes) project to
    your machine.
1.  Set the `utils` directory to world-readable: `chmod -R 755 ./utils`.
1.  Build binaries by running `mvn clean install` from the root directory of the
    repository.

## Run the pipeline

Run the pipeline directly using the `java` command:

```
java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://example.org/fhir \
    --[see additional parameters below]
```

Add the necessary parameters depending on your use case. The methods used for
reading the source FHIR server and outputting the data depend on the parameters
used. You can output to both Parquet files and a FHIR server by including the
required parameters for both.

## Parameters

This section documents the parameters used by the various pipelines. For more
information on parameters, see
[`FhirEtlOptions`](https://github.com/google/fhir-data-pipes/blob/master/pipelines/batch/src/main/java/org/openmrs/analytics/FhirEtlOptions.java).

### Common parameters

These parameters are used regardless of other pipeline options.

- `fhirServerUrl` - The base URL of the source FHIR server. Required.
- `fhirServerUserName` - The HTTP Basic Auth username to access the FHIR server
  APIs. Default: `admin`
- `fhirServerPassword` - The HTTP Basic Auth password to access the FHIR server
  APIs. Default: `Admin123`
- `resourceList` - A comma-separated list of
  [FHIR resources](https://www.hl7.org/fhir/resourcelist.html) to include in the
  pipeline. Default: `Patient,Encounter,Observation`
- `runner` -
  [The Apache Beam Runner](https://beam.apache.org/documentation/runners/capability-matrix/)
  to use. Pipelines supports DirectRunner and FlinkRunner by default; other
  runners can be enabled by Maven profiles, e.g.,
  [DataflowRunner](https://github.com/google/fhir-data-pipes/blob/16fcc255cef4d2708b9941a854e6c638b2533d45/pipelines/batch/pom.xml#L257).
  Default: `DirectRunner`

### FHIR-Search input parameters

The pipeline will use FHIR-Search to fetch data as long as `jdbcModeEnabled` is
unset or false.

- `batchSize` - The number of resources to fetch in each API call. Default:
  `100`

### JDBC input parameters

JDBC mode is used if `jdbcModeEnabled=true`.

To use JDBC mode, first create a copy of
[hapi-postgres-config.json](https://github.com/google/fhir-data-pipes/blob/master/utils/hapi-postgres-config.json)
and edit the values to match your database server.

Next, include the following parameters:

- `jdbcModeEnabled=true`
- `fhirDatabaseConfigPath=./path/to/config.json`

If you are using a HAPI FHIR server, also include:

- `jdbcModeHapi=true`
- `jdbcDriverClass=org.postgresql.Driver`

All JDBC parameters:

- `jdbcModeEnabled` - If true, uses JDBC mode. Default: `false`
- `fhirDatabaseConfigPath` - Path to the FHIR database config for JDBC mode.
  Default: `../utils/hapi-postgres-config.json`
- `jdbcModeHapi` - If true (with `jdbcModeEnabled`), uses JDBC mode for a HAPI
  source server. Default: `false`
- `jdbcFetchSize` - The fetch size of each JDBC database query. Default: `10000`
- `jdbcMaxPoolSize` - The maximum number of database connections. Default: `50`
- `jdbcDriverClass` - The JDBC driver to use. Should be set to
  `org.postgresql.Driver` for HAPI FHIR Postgres access. Default:
  `com.mysql.cj.jdbc.Driver`

### Parquet output parameters

Parquet files are output when `outputParquetPath` is set.

- `outputParquetPath` - The file path to write Parquet files to, e.g.,
  `./tmp/parquet/`. Default: empty string, which does not output Parquet files.
- `secondsToFlushParquetFiles` - The number of seconds to wait before flushing
  all Parquet writers with non-empty content to files. Use `0` to disable.
  Default: `3600`.
- `rowGroupSizeForParquetFiles` - The approximate size in bytes of the
  row-groups in Parquet files. When this size is reached, the content is flushed
  to disk. This is not used if there are less than 100 records. Use `0` to use
  the default Parquet row-group size. Default: `0`.

### FHIR output parameters

Resources will be copied to the FHIR server specified in `fhirSinkPath` if that
field is set.

- `fhirSinkPath` - A base URL to a target FHIR server, or the relative path of a
  GCP FHIR store, e.g. `http://localhost:8091/fhir` for a FHIR server or
  `projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIR-STORE-NAME`
  for a GCP FHIR store. If using a GCP FHIR store,
  [see here for setup information](https://github.com/google/fhir-data-pipes/wiki/Create-a-Google-Cloud-FHIR-Store-and-BigQuery-Dataset).
  default: none, resources are not copied
- `sinkUserName` - The HTTP Basic Auth username to access the FHIR sink. Not
  used for GCP FHIR stores.
- `sinkPassword` - The HTTP Basic Auth password to access the FHIR sink. Not
  used for GCP FHIR stores.

### A note about Beam runners

If the pipeline is run on a single machine (i.e., not on a distributed cluster),
for large datasets consider using a production grade runner like Flink. This can
be done by adding the parameter `--runner=FlinkRunner` (use `--maxParallelism`
and `--parallelism` to control parallelism). This should not give a significant
run time improvement but may avoid some of the memory issues of `DirectRunner`.

## Example configurations

These examples are set up to work with
[local test servers](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers).

### FHIR Search to Parquet files

Example run:

```shell
java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8091/fhir \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation
```

### HAPI FHIR JDBC to a FHIR server

Example run:

```shell
java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8091/fhir \
    --resourceList=Patient,Encounter,Observation \
    --fhirDatabaseConfigPath=./utils/hapi-postgres-config.json \
    --jdbcModeEnabled=true --jdbcModeHapi=true \
    --jdbcMaxPoolSize=50 --jdbcFetchSize=1000 \
    --jdbcDriverClass=org.postgresql.Driver \
    --fhirSinkPath=http://localhost:8099/fhir \
    --sinkUserName=hapi --sinkPassword=hapi
```

## How to query the data warehouse

To query Parquet files, load them into a compatible data engine such as Apache
Spark. The
[single machine Docker Compose configuration](https://github.com/google/fhir-data-pipes/wiki/Analytics-on-a-single-machine-using-Docker)
runs the pipeline and loads data into an Apache Spark Thrift server for you.
