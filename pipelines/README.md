# Batch and streaming transformation of FHIR resources

This directory contains pipelines code for batch and streaming transformation of
data from a FHIR based EHR system to a data warehouse (for analytics) or another
FHIR store (for data integration). These pipelines are tested with
[OpenMRS](https://openmrs.org) instances as the source using the
[OpenMRS FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module).
These pipelines are also tested with [HAPI](https://hapifhir.io/) instances as
the source. The data warehouse can be a collection of
[Apache Parquet files](https://parquet.apache.org), or it can be another FHIR
server (e.g., a HAPI FHIR server or Google Cloud FHIR store) or eventually a
cloud based datawarehouse (like [BigQuery](https://cloud.google.com/bigquery)).

There are four modes of transfer:

- [**Batch mode (FHIR)**](#batch-mode-using-fhir-search): This mode uses FHIR
  Search APIs to select resources to copy, retrieves them as FHIR resources, and
  transfers the data via FHIR APIs or Parquet files.
- [**Batch mode (JDBC)**](#batch-mode-using-jdbc): For an OpenMRS source, this
  mode uses JDBC to read the IDs of entities in an OpenMRS MySQL database,
  retrieves those entities as FHIR resources, and transfers the data via FHIR
  APIs or Parquet files. For a HAPI source, this mode uses JDBC to read FHIR
  resources directly from a HAPI database, and transfers the data.
- **[Streaming mode (Debezium)](#streaming-mode-debezium)**: This mode
  continuously listens for changes to the underlying OpenMRS MySQL database
  using
  [Debezium](https://debezium.io/documentation/reference/1.2/connectors/mysql.html).

There is also a query module built on top of the generated data warehouse.

## Prerequisites

- An [OpenMRS](https://openmrs.org) instance with the latest version of the
  [FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
  installed, or a [HAPI](https://hapifhir.io/) instance.
  - There is an [OpenMRS Reference Application](#run-openmrs-using-docker) image
    with these prerequisites and demo data you can use to try things out.
  - There is a [HAPI FHIR source server and database](/docker/hapi-compose.yml)
    image you can use for testing.
- A target output for the data. Supported options are Apache Parquet files or a
  FHIR server such as HAPI FHIR or
  [Google Cloud Platform FHIR stores](https://cloud.google.com/healthcare/docs/how-tos/fhir).
  - You can use our [HAPI FHIR server](#run-hapi-fhir-server-using-docker) image
    for testing FHIR API targets.
  - [Learn how to create a compatible GCP FHIR store](#create-a-google-cloud-fhir-store-and-bigquery-dataset),
    if you want to use this option.
- Build the [local version of Bunsen](bunsen/)

### Installing the FHIR2 Module

**To install via web**: Log in to OpenMRS as an administrator, go to **System
Administration > Manage Modules > Search from Addons**, search for the `fhir2`
module, and install. Make sure to install the `fhir2` module, not `fhir`.

**To install via .omod file**: Go to
[FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
and download the .omod file. Go to **Advanced Administration > Module
Properties** and find the modules folder. Move the .omod file to the modules
folder then restart OpenMRS.

[Learn more about OpenMRS modules](https://guide.openmrs.org/en/Configuration/customizing-openmrs-with-plug-in-modules.html).

## Setup

1.  [Install the FHIR2 Module](#installing-the-fhir2-module) if necessary.
2.  [Clone](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository)
    the
    [OpenMRS FHIR Analytics](https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics)
    project to your machine.
3.  Set the `utils` directory to world-readable: `chmod -r 755 ./utils`.
4.  Build binaries with `mvn clean install`.

## Common Parameters

Although each mode of transfer uses a different binary, they use some common
parameters which are documented here.

- `fhirServerUrl` - The base URL of the source fhir server instance. Default:
  `http://localhost:8099/openmrs/ws/fhir2/R4`
- `fhirServerUserName` - The HTTP Basic Auth username to access the fhir server
  APIs. Default: `admin`
- `fhirServerPassword` - The HTTP Basic Auth password to access the fhir server
  APIs. Default: `Admin123`
- `fhirSinkPath` - A base URL to a target FHIR server, or the relative path of a
  GCP FHIR store, e.g. `http://localhost:8098/fhir` for a FHIR server or
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
default it uses [FHIR Search](https://www.hl7.org/fhir/search.html) API calls to
find the resources to copy. Alternatively, it can use
[JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) to query the
FHIR database directly.

### Using Batch mode

First, complete the [Setup](#setup) instructions.

The next sections describe parameters specific to Batch mode. See
[Common Parameters](#common-parameters) for information about the other
parameters.

### Batch mode using FHIR Search with OpenMRS as the source

To start Batch Mode using FHIR Search, run:

```shell
$ java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
    --fhirServerUserName=admin --fhirServerPassword=Admin123 \
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

### Batch mode using FHIR Search with HAPI as the source

To start Batch Mode using FHIR Search, run:

```shell
$ java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8098/fhir \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation --batchSize=20
```

### Batch mode using JDBC with OpenMRS as the source

To start Batch Mode using JDBC, run:

```shell
$ java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar \
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

Parameters:

- `resourceList` - A comma-separated list of FHIR Resources to be fetched from
  OpenMRS. Default: `Patient,Encounter,Observation`
- `batchSize` - The number of resources to fetch in each API call. Default:
  `100`
- `jdbcModeEnabled` - If true, uses JDBC mode. Default: `false`
- `jdbcMaxPoolSize` - The maximum number of database connections. Default: `50`
- `jdbcDriverClass` - The fully qualified class name of the JDBC driver. This
  generally should not be changed. Default: `com.mysql.cj.jdbc.Driver`

### Batch mode using JDBC with HAPI as the source

To start Batch Mode using JDBC, run:

```shell
$ java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar \
    org.openmrs.analytics.FhirEtl \
    --fhirServerUrl=http://localhost:8091/fhir \
    --fhirServerUserName=hapi --fhirServerPassword=hapi \
    --fhirDatabaseConfigPath=../utils/hapi-postgres-config.json
    --fhirSinkPath=http://localhost:8098/fhir \
    --sinkUserName=hapi --sinkPassword=hapi \
    --outputParquetPath=/tmp/TEST/ \
    --resourceList=Patient,Encounter,Observation \
    --jdbcModeEnabled=true --jdbcModeHapi=true \
    --jdbcMaxPoolSize=50 --jdbcFetchSize=1000 \
    --jdbcDriverClass=org.postgresql.Driver

```

Parameters:

- `fhirDatabaseConfigPath` - Path to the FHIR database config for Jdbc mode.  
  Default: `../utils/hapi-postgres-config.json`
- `jdbcModeHapi` - If true, uses JDBC mode for a HAPI source server. Default:
  `false`
- `jdbcFetchSize` - The fetch size of each JDBC database query. Default: `10000`

### A note about Beam runners

If the pipeline is run on a single machine (i.e., not on a distributed cluster),
for large datasets consider using a production grade runner like Flink. This can
be done by adding `--runner=FlinkRunner` to the above command lines (use
`--maxParallelism` and `--parallelism` to control parallelism). For our
particular case, this should not give a significant run time improvement but may
avoid some of the memory issues of Direct runner.

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

1.  Complete the Debezium
    [MySQL Setup](https://debezium.io/documentation/reference/1.4/connectors/mysql.html#setting-up-mysql).
    At a minimum,
    [create a user](https://debezium.io/documentation/reference/1.4/connectors/mysql.html#mysql-creating-user)
    and
    [enable the binlog](https://debezium.io/documentation/reference/1.4/connectors/mysql.html#enable-mysql-binlog).
    (The test [Docker image](#run-openmrs-and-mysql-using-docker) has already
    done these steps.)
2.  Edit `../utils/dbz_event_to_fhir_config.json`. Find the
    `debeziumConfigurations` section at the top of the file and edit the values
    to match your environment. See the documentation on
    [Debezium MySQL Connector properties](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-property-name)
    for more information.
3.  Build binaries with `mvn clean install`.
4.  Run the pipeline to a FHIR server and Parquet files:

    ```shell
    $ mvn compile exec:java -pl streaming \
        -Dexec.args="--fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
        --fhirServerUserName=admin --fhirServerPassword=Admin123 \
        --fhirSinkPath=http://localhost:8098/fhir \
        --sinkUserName=hapi --sinkPassword=hapi \
        --outputParquetPath=/tmp/TEST/ \
        --fhirDebeziumConfigPath=../utils/dbz_event_to_fhir_config.json"
    ```

    Or to a GCP FHIR store:

    ```shell
    $ mvn compile exec:java -pl streaming \
        -Dexec.args="--fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
        --fhirServerUserName=admin --fhirServerPassword=Admin123 \
        --fhirSinkPath=projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME \
        --fhirDebeziumConfigPath=../utils/dbz_event_to_fhir_config.json"
    ```

The next sections describe parameters specific to Debezium Streaming mode. See
[Common Parameters](#common-parameters) for information about the other
parameters.

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

- **Will this pipeline include historical data recorded before the
  `mysql binlog` was enabled?** Yes. By default, the pipeline takes a snapshot
  of the entire database and will include all historical data.

- **How do I stop Debezium from taking a snapshot of the entire database?** Set
  `snapshotMode` in the config file to `schema_only` i.e,
  `"snapshotMode" : "initial"`. Other options include: `when_needed`,
  `schema_only`, `initial` (default), `never`, e.t.c. See the
  [`debezium documentation`](https://camel.apache.org/components/latest/debezium-mysql-component.html)
  for more details.

## Using Docker compose

You can run the entire pipeline using Docker containers by following these
instructions.

### Run OpenMRS and MySQL using Docker

```
$ docker-compose -f docker/openmrs-compose.yaml up
```

Once running you can access OpenMRS at <http://localhost:8099/openmrs/> using
username "admin" and password "Admin123". The Docker image includes the required
FHIR2 module and demo data. Edit `docker/openmrs-compose.yaml` to change the
default port.

**Note:** If `docker-compose` fails, you may need to adjust file permissions. In
particular if the permissions on `mysqld.cnf` is not right, the `datadir` set in
this file will not be read by MySQL and it will cause OpenMRS to require its
`initialsetup` (which is not needed since the MySQL image already has all the
data and tables needed):

```
$ docker-compose -f docker/openmrs-compose.yaml down -v
$ chmod a+r docker/mysql-build/mysqld.cnf
$ chmod -R a+r ./utils
$ docker-compose -f docker/openmrs-compose.yaml up
```

In order to see the demo data in OpenMRS you must rebuild the search index. In
OpenMRS go to **Home > System Administration > Advanced Administration**. Under
**Maintenance** go to **Search Index** then **Rebuild Search Index**.

### (Optional) Run HAPI FHIR server using Docker

This is only needed if you want to test sending data to a FHIR server and is not
needed if you are only generating Parquet files.

```
$ docker-compose -f docker/sink-compose.yml up
```

You can access the FHIR server at <http://localhost:8098/fhir/>. Edit
`sink-compose.yml` to change the default port.

### Configure docker-compose.yaml

Parameters (e.g port/url) have been configured to point to
http://localhost:8099/openmrs/

Remember to appropriately change other parameters such as source url, sink path,
and any required authentication information.

### Run a Transfer Pipeline

#### Batch Pipeline

```
$ mvn clean install
$ docker-compose -f docker/docker-compose.yaml up --build batch
```

##### Streaming Pipeline (Debezium)

```
$ mvn clean install
$ docker-compose -f docker/docker-compose.yaml up --build streaming
```

## How to query the data warehouse

See [`dwh` directory](../dwh).
