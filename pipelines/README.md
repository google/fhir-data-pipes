# Batch and streaming transformation of FHIR resources

This directory contains pipelines code for batch and streaming transformation of
data from a FHIR-based source server to a data warehouse (for analytics) or
another FHIR store (for data integration). These pipelines are tested with
[HAPI](https://hapifhir.io/) instances as the source or
[OpenMRS](https://openmrs.org) instances as the source using the
[OpenMRS FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module).
The data warehouse can be a collection of
[Apache Parquet files](https://parquet.apache.org), or it can be another FHIR
server (e.g., a HAPI FHIR server or Google Cloud FHIR store) or eventually a
cloud based data warehouse (like [BigQuery](https://cloud.google.com/bigquery)).

There are four modes of transfer:

- [**Batch mode (FHIR-search)**](#batch-mode-using-fhir-search): This mode uses
  FHIR Search APIs to select resources to copy, retrieves them as FHIR
  resources, and transfers the data via FHIR APIs or Parquet files.
- [**Batch mode (JDBC)**](#batch-mode-using-jdbc): For a HAPI source, this mode
  uses JDBC to read FHIR resources directly from a HAPI database, and transfers
  the data. For an OpenMRS source, this mode uses JDBC to read the IDs of
  entities in an OpenMRS database, retrieves those entities as FHIR resources,
  and transfers the data via FHIR APIs or Parquet files.
- **[Streaming mode (Debezium)](#streaming-mode-debezium)**: This mode
  continuously listens for changes to the underlying OpenMRS MySQL database
  using
  [Debezium](https://debezium.io/documentation/reference/1.2/connectors/mysql.html).

There is also a query module built on top of the generated data warehouse.

## Pre-requisites

**Note**: All commands are run from the git repository's root directory

- Create an external Docker network named `cloudbuild` to run Docker images
  listed below:
  ```
  docker network create cloudbuild
  ```
- Bring up FHIR source server. Either a:

  - [HAPI source server and database](../docker/hapi-compose.yml) image. The
    sample docker-compose file that is provided, brings up a standard HAPI JPA
    server backed by a PostgreSQL database. The base URL for this server is
    `http://localhost:8081/fhir`. Run:
    ```shell
    docker-compose  -f ./docker/hapi-compose.yml up  --force-recreate -d
    ```
  - [OpenMRS Reference Application](../docker/openmrs-compose.yaml) image. The
    sample docker-compose file that is provided, brings up the latest OpenMRS
    release backed by a MySQL database. The base URL for this server is
    `http://localhost:8099/openmrs/ws/fhir2/R4`
    ```shell
    docker-compose -f ./docker/openmrs-compose.yml up --force-recreate -d
    ```

- Upload the Synthetic data stored in [sample_data](../synthea-hiv/sample_data)
  to the FHIR server that you brought up using the
  [Synthea Uploader](../synthea-hiv/README.md#Uploader). For example, to upload
  to the HAPI FHIR server brought up in the previous step, run:
  ```shell
  python3 ./synthea-hiv/uploader/main.py HAPI http://localhost:8091/fhir \
  --input_dir ./synthea-hiv/sample_data
  ```
- [Optional] If the output is not only Apache Parquet files, and you want to
  output to a FHIR sink server, the sample docker-compose file that is provided,
  [HAPI sink server](../docker/sink-compose.yml), brings up a plain HAPI server
  . The base URL for this server is `http://localhost:8098/fhir`. Run:
  ```shell
  docker-compose  -f ./docker/sink-compose.yml up  --force-recreate -d
  ```
- Build the pipelines:
  ```shell
  mvn clean package
  ```

## Common Parameters

Although each mode of transfer uses a different binary, they use some common
parameters which are documented here.

- `fhirServerUrl` - The URL of the source fhir server instance
- `fhirServerUserName` - The HTTP Basic Auth username to access the fhir server
  APIs.
- `fhirServerPassword` - The HTTP Basic Auth password to access the fhir server
  APIs.
- `fhirSinkPath` - A base URL to a target FHIR server, or the relative path of a
  GCP FHIR store, e.g. `http://localhost:8098/fhir` for the FHIR server
  specified for sink or
  `projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIR-STORE-NAME`
  for a GCP FHIR store. If you don't need to push to a sink FHIR server, you can
  leave this flag empty which is the default.
- `sinkUserName` - The HTTP Basic Auth username to access the FHIR sink. Not
  used for GCP FHIR stores.
- `sinkPassword` - The HTTP Basic Auth password to access the FHIR sink. Not
  used for GCP FHIR stores.
- `outputParquetPath` - The file path to write Parquet files to, e.g.,
  `./tmp/parquet/`. If you don't need to write to Parquet, you can leave this
  flag empty which is the default.

## Batch mode

Batch mode can use two different methods to query the source database. By
default, it uses [FHIR Search](https://www.hl7.org/fhir/search.html) API calls
to find the resources to copy. Alternatively, it can use
[JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) to query the
FHIR database directly.

### Using Batch mode

First, complete the [Pre-requisites](#pre-requisites) instructions.

The next sections describe parameters specific to Batch mode. See
[Common Parameters](#common-parameters) for information about the other
parameters.

### Batch mode using FHIR Search

To start Batch Mode using FHIR Search, extracting from a HAPI FHIR server and
loading to a different FHIR server and to Parquet files, run:

```shell
$ java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8091/fhir \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation --batchSize=20
```

Parameters:

- `resourceList` - A comma-separated list of
  [FHIR resources](https://www.hl7.org/fhir/resourcelist.html) URLs. For
  example, `Patient?given=Susan` will extract only Patient resources that meet
  the `given=Susan` criteria. Default: `Patient,Encounter,Observation`
- `batchSize` - The number of resources to fetch in each API call. Default:
  `100`

And here is the command to run the pipeline with OpenMRS as the source server:

```shell
$ java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
    --fhirServerUserName=admin --fhirServerPassword=Admin123 \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation --batchSize=20
```

### Batch mode using JDBC

To start Batch Mode using JDBC, extracting from a HAPI FHIR server and loading
to a different FHIR server and to Parquet files, first update the value of the
`databaseHostName` to `"localhost"` in
[hapi-postgres-config.json](../utils/hapi-postgres-config.json), then run:

:

```shell
$ java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8091/fhir \
    --fhirServerUserName=hapi --fhirServerPassword=hapi \
    --fhirDatabaseConfigPath=./utils/hapi-postgres-config.json \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation \
    --jdbcModeEnabled=true --jdbcModeHapi=true \
    --jdbcMaxPoolSize=50 --jdbcFetchSize=1000 \
    --jdbcDriverClass=org.postgresql.Driver

```

Parameters:

- `resourceList` - A comma-separated list of
  [FHIR resources](https://www.hl7.org/fhir/resourcelist.html) URLs. For
  example, `Patient?given=Susan` will extract only Patient resources that meet
  the `given=Susan` criteria. Default: `Patient,Encounter,Observation`
- `batchSize` - The number of resources to fetch in each API call. Default:
  `100`
- `fhirDatabaseConfigPath` - Path to the FHIR database config for Jdbc mode.
- `jdbcModeHapi` - If true, uses JDBC mode for a HAPI source server. Default:
  `false`
- `jdbcFetchSize` - The fetch size of each JDBC database query. Default: `10000`
- `jdbcModeEnabled` - If true, uses JDBC mode. Default: `false`
- `jdbcMaxPoolSize` - The maximum number of database connections. Default: `50`
- `jdbcDriverClass` - The fully qualified class name of the JDBC driver. This
  generally should not be changed. Default: `com.mysql.cj.jdbc.Driver`

To start Batch Mode using JDBC to extract resources from OpenMRS, run:

```shell
$ java -cp ./pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
    --fhirServerUserName=admin --fhirServerPassword=Admin123 \
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation --batchSize=20 \
    --jdbcModeEnabled=true --jdbcMaxPoolSize=50 \
    --jdbcDriverClass=com.mysql.cj.jdbc.Driver
```

### A note about Beam runners

If the pipeline is run on a single machine (i.e., not on a distributed cluster),
for large datasets consider using a production grade runner like Flink. This can
be done by adding `--runner=FlinkRunner` to the above command lines (use
`--maxParallelism` and `--parallelism` to control parallelism). For our
particular case, this should not give a significant run time improvement but may
avoid some memory issues of Direct runner.

## Streaming mode (Debezium)

The Debezium-based streaming mode provides real-time downstream consumption of
incremental updates, even for operations that were performed outside the OpenMRS
API, e.g., data cleaning, module operations, and data syncing/migration. It
captures incremental updates from the MySQL database binlog then streams both
FHIR and non-FHIR data for downstream consumption. It is not based on Hibernate
Interceptor or Event Module; therefore, all events are captured from day 0 and
can be used independently without the need for a batch pipeline. It also
tolerates failures like application restarts or crashes, as the pipeline will
resume from the last processed offset.

### Getting Started

1.  Edit `../utils/dbz_event_to_fhir_config.json`. Find the
    `debeziumConfigurations` section at the top of the file and edit the values
    to match your environment. See the documentation on
    [Debezium MySQL Connector properties](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-property-name)
    for more information.
2.  Build binaries with `mvn clean install`.
3.  Run the pipeline to a FHIR server and Parquet files:

    ```shell
    $ java -cp ./pipelines/streaming/target/streaming-bundled-0.1.0-SNAPSHOT.jar \
      org.openmrs.analytics.Runner \
      --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
        --fhirServerUserName=admin --fhirServerPassword=Admin123 \
        --fhirSinkPath=http://localhost:8098/fhir \
        --sinkUserName=hapi --sinkPassword=hapi \
        --outputParquetPath=/tmp/TEST/ \
        --fhirDebeziumConfigPath=./utils/dbz_event_to_fhir_config.json
    ```

    Or to a GCP FHIR store:

    ```shell
    $ java -cp ./pipelines/streaming/target/streaming-bundled-0.1.0-SNAPSHOT.jar \
      org.openmrs.analytics.Runner \
      --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
        --fhirServerUserName=admin --fhirServerPassword=Admin123 \
        --fhirSinkPath=projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME \
        --fhirDebeziumConfigPath=../utils/dbz_event_to_fhir_config.json
    ```

Parameters:

- `fhirDebeziumConfigPath` - The path to the configuration file containing MySQL
  parameters and FHIR mappings. This generally should not be changed. Default:
  `../utils/dbz_event_to_fhir_config.json`

If `outputParquetPath` is set, there are additional parameters:

- `secondsToFlushParquetFiles` - The number of seconds to wait before flushing
  all Parquet writers with non-empty content to files. Use `0` to disable.
  Default: `3600`.
- `rowGroupSizeForParquetFiles` - The approximate size in bytes of the
  row-groups in Parquet files. When this size is reached, the content is flushed
  to disk. This is not used if there are less than 100 records. Use `0` to use
  the default Parquet row-group size. Default: `0`.

### Common questions

- **Do I need to have a local HAPI server to load to?** No, this is only needed
  if you want to test sending data to a local HAP FHIR server and is not needed
  if you are only generating Parquet files.

- **Will this pipeline include historical data recorded before the
  `mysql binlog` was enabled?** Yes. By default, the pipeline takes a snapshot
  of the entire database and will include all historical data.

- **How do I stop Debezium from taking a snapshot of the entire database?** Set
  `snapshotMode` in the config file to `schema_only` i.e,
  `"snapshotMode" : "initial"`. Other options include: `when_needed`,
  `schema_only`, `initial` (default), `never`, e.t.c. See the
  [`debezium documentation`](https://camel.apache.org/components/latest/debezium-mysql-component.html)
  for more details.

- **What do I do if running `docker-compose` to stand up the OpenMRS image
  fails** If `docker-compose` fails, you may need to adjust file permissions. In
  particular if the permissions on `mysqld.cnf` is not right, the `datadir` set
  in this file will not be read by MySQL, and it will cause OpenMRS to require
  its `initialsetup` (which is not needed since the MySQL image already has all
  the data and tables needed):

  ```shell
  $ docker-compose -f docker/openmrs-compose.yaml down -v
  $ chmod a+r docker/mysql-build/mysqld.cnf
  $ chmod -R a+r ./utils
  $ docker-compose -f docker/openmrs-compose.yaml up
  ```

- **How do I see the demo data in OpenMRS?** In order to see the demo data in
  OpenMRS you must rebuild the search index. In OpenMRS go to **Home > System
  Administration > Advanced Administration**. Under **Maintenance** go to
  **Search Index** then **Rebuild Search Index**.

## How to query the data warehouse

See [`dwh` directory](../dwh).
