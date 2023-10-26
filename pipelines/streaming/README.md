# OpenMRS Streaming Pipeline

Note: This pipeline is in maintenance mode and not actively developed. There are
known issues with duplicated resources in Parquet files.

The
[streaming](https://github.com/google/fhir-data-pipes/tree/master/pipelines/streaming)
directory contains code to continuously listen for changes to an underlying
OpenMRS database using
[Debezium](https://debezium.io/documentation/reference/1.2/connectors/mysql.html).

The Debezium-based streaming mode provides real-time downstream consumption of
incremental updates, even for operations that were performed outside the OpenMRS
API, e.g., data cleaning, module operations, and data syncing/migration. It
captures incremental updates from the MySQL database binlog then streams both
FHIR and non-FHIR data for downstream consumption. It is not based on Hibernate
Interceptor or Event Module; therefore, all events are captured from day 0 and
can be used independently without the need for a batch pipeline. It also
tolerates failures like application restarts or crashes, as the pipeline will
resume from the last processed offset.

Streaming mode works only with OpenMRS; support for other data sources is not
planned.

## Prerequisites

- An [OpenMRS](https://openmrs.org) instance with the latest version of the
  [FHIR2 Module](https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
  installed.
  - There is an
    [OpenMRS Reference Application](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers)
    image with these prerequisites and demo data you can use to try things out.
- A target output for the data. Supported options are Apache Parquet files or a
  FHIR server such as HAPI FHIR or
  [Google Cloud Platform FHIR stores](https://cloud.google.com/healthcare/docs/how-tos/fhir).
  - You can use our [HAPI FHIR server](#run-hapi-fhir-server-using-docker) image
    for testing FHIR API targets.
  - [Learn how to create a compatible GCP FHIR store](https://github.com/google/fhir-data-pipes/wiki/Create-a-Google-Cloud-FHIR-Store-and-BigQuery-Dataset),
    if you want to use this option.

## Getting Started

1.  [Clone](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository)
    the [FHIR Data Pipes](https://github.com/google/fhir-data-pipes) project to
    your machine.
1.  Set the `utils` directory to world-readable: `chmod -R 755 ./utils`.
1.  Edit `../utils/dbz_event_to_fhir_config.json`. Find the
    `debeziumConfigurations` section at the top of the file and edit the values
    to match your environment. See the documentation on
    [Debezium MySQL Connector properties](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-property-name)
    for more information.
1.  Build binaries from the repo root with `mvn clean package`.
1.  Run the pipeline to a FHIR server and Parquet files:

    ```shell
    $ java -jar ./pipelines/streaming/target/streaming-bundled.jar \
      com.google.fhir.analytics.Runner \
      --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
        --fhirServerUserName=admin --fhirServerPassword=Admin123 \
        --fhirSinkPath=http://localhost:8098/fhir \
        --sinkUserName=hapi --sinkPassword=hapi \
        --outputParquetPath=/tmp/TEST/ \
        --fhirDatabaseConfigPath=./utils/dbz_event_to_fhir_config.json
    ```

    Or to a GCP FHIR store:

    ```shell
    $ java -jar ./pipelines/streaming/target/streaming-bundled.jar \
      com.google.fhir.analytics.Runner \
      --fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4 \
        --fhirServerUserName=admin --fhirServerPassword=Admin123 \
        --fhirSinkPath=projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME \
        --fhirDatabaseConfigPath=../utils/dbz_event_to_fhir_config.json
    ```

## Parameters

### Basic parameters

- `fhirServerUrl` - The URL of the source FHIR server instance.
- `fhirServerUserName` - The HTTP Basic Auth username to access the fhir server
  APIs.
- `fhirServerPassword` - The HTTP Basic Auth password to access the fhir server
  APIs.

### Debezium parameters

- `fhirDatabaseConfigPath` - The path to the configuration file containing MySQL
  parameters and FHIR mappings. This generally should not be changed. Instead,
  edit `../utils/dbz_event_to_fhir_config.json` to match your configuration.
  Default: `../utils/dbz_event_to_fhir_config.json`

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

## Common questions

- **Will this pipeline include historical data recorded before the
  `mysql binlog` was enabled?** Yes. By default, the pipeline takes a snapshot
  of the entire database and will include all historical data.

- **How do I stop Debezium from taking a snapshot of the entire database?** Set
  `snapshotMode` in the config file to `schema_only` i.e.,
  `"snapshotMode" : "initial"`. Other options include: `when_needed`,
  `schema_only`, `initial` (default), `never`, e.t.c. See the
  [`debezium documentation`](https://camel.apache.org/components/latest/debezium-mysql-component.html)
  for more details.
