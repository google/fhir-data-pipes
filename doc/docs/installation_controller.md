# Deploy the Controller Module

!!! tip "Guide Overview"

    * This guide will walk you through how to deploy and run the Pipelines Controller Module
    * This module is designed to work with the FHIR Data Pipes ETL Pipelines
    * For ease of deployment a set of example docker compose configurations that include the Pipeline and Controller has been provided. See the Docker section

## Usage

The FHIR Pipelines Controller helps you schedule and manage the running of the
FHIR Data Pipes Pipeline. Using the Controller module you can configure the
Pipeline to run either full or incremental transformations, and it can also be
used to monitor the pipelines.

The Controller acts as a simplified entry point to managing the Data Pipes
Pipelines for less experienced users and non-developers, all the configurations
and features supported by the controller are also available by running the
[ETL Pipelines](installation_pipeline.md#setup) as a Java JAR.

## Setup

1. Clone the
   [fhir-data-pipes GitHub repository](https://github.com/google/fhir-data-pipes),
   open a terminal window.
2. `cd` to the directory where you cloned it.
3. Change to the `controller` directory: `cd pipelines/controller/`.

Later terminal commands will assume your working directory is the `controller`
directory.

<br>

Next, configure the FHIR Pipelines Controller. The controller relies on several
configuration files to run. Edit them to match your environment and
requirements. For sample values that are used in
[single machine deployment examples](tutorials/single_machine) see
[docker/config](https://github.com/google/fhir-data-pipes/tree/master/docker/config).

- **Main settings for Pipeline Controller**:
  [`pipelines/controller/config/application.yaml`](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/application.yaml).
  Edit the values to match your FHIR server and use case.
- **JDBC settings**:
  [`pipelines/controller/config/hapi-postgres-config.json`](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/hapi-postgres-config.json).
  If you are using the JDBC mode to read resources, edit the values to match the
  database of your HAPI FHIR server. The JDBC mode is not implemented for other
  FHIR servers.
- **Customized FlinkRunner settings**:
  [`pipelines/controller/config/flink-conf.yaml`](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/flink-conf.yaml).
  To use this file, set the
  [`FLINK_CONF_DIR` environmental variable](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/):
  `export FLINK_CONF_DIR=[PATH]/flink-conf.yaml`. Changing
  `taskmanager.memory.network.max` is necessary to avoid memory errors for large
  datasets or when running on machines with limited memory.
- **Spark Server settings**:
  [`pipelines/controller/config/thriftserver-hive-config.json`](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/thriftserver-hive-config.json).
  Used to connect to a Spark server when
  [`createHiveResourceTables`](https://github.com/google/fhir-data-pipes/blob/42c5e309b37faf6f61b0e5d3f993c6e7d448974c/pipelines/controller/config/application.yaml#L121)
  is set to `true`. Edit the values to match your Spark server if necessary.

## Run the FHIR Pipelines Controller

The Pipelines Controller is run from the
[FHIR Pipelines Control Panel](additional#web-control-panel).

!!! warning "Warning"

    Note: The Pipelines Controller is not intended to be a security barrier. As such the control panel does not support user authorization or any advanced security features.

There are 2 ways to run the FHIR Pipelines Controller from the web application
control panel.

Using **Spring Boot**:

```
mvn spring-boot:run
```

Running the **JAR** directly:

```
mvn clean install
java -jar ./target/controller-bundled.jar
```

After running, open a web browser and visit http://localhost:8080. You should
see the FHIR Pipelines Control Panel.

There are 3 ways to have the FHIR Pipelines Controller run the transformation
pipeline:

- Manually trigger the **Run Full** option by clicking the button. This
  transforms all the selected FHIR resource types to Parquet files. You are
  **required** to use this option once before using any of the following
  incremental options.
- Manually trigger the **Run Incremental** option by clicking the button. This
  only outputs resources that are new or changed since the last run.
- Automatically scheduled incremental runs, as specified by
  [`incrementalSchedule`](#advanced-configurations) parameter. You can see when
  the next scheduled run is near the top of the control panel.

  **Note:** The bottom area of the control panel shows the current options being
  used by the Pipelines Controller.

<br>

After running the pipeline, look for the Parquet files created in the directory
specified by `dwhRootPrefix` in the
[application.yaml](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/application.yaml)
file.

## Explore the configuration settings

The parameters described in this section correspond to the settings in the
[application.yaml](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/application.yaml)
file. Please reference this file for comprehensive information on each
configuration parameter

### FHIR Search Input Parameters

- `fhirServerUrl`: Base URL of the source FHIR server. If `dbConfig` is not set,
  resources are fetched from this URL through the FHIR Search API. e.g.
  `http://172.17.0.1:8091/fhir`
- `fhirServerUserName`, `fhirServerPassword`: The user-name/password should be
  set if the FHIR server supports Basic Auth. Default: `admin`, `Admin123`

### Authentication

The following client credentials should be set if the source FHIR server accepts
OAuth access tokens. Note the client credentials (the secret) are sensitive, and
it is probably a better practice to set these through command-line arguments
[here](additional.md#authentication).

- `fhirServerOAuthTokenEndpoint`
- `fhirServerOAuthClientId`
- `fhirServerOAuthClientSecret`

### JDBC Input Parameters

- `dbConfig`: The path to the file containing JDBC settings for connecting to a
  HAPI FHIR server database. If this is set, resources are fetched directly from
  the database and `fhirServerUrl` is ignored. Equivalent to pipeline
  `fhirDatabaseConfigPath` parameter.

- `dwhRootPrefix`: The path to output Parquet files to. The last portion of the
  path is used as a prefix for naming the directory that contains per-resource
  directories with an added timestamp.

!!! info "Info"

    Visit the [application.yaml](https://github.com/google/fhir-data-pipes/tree/master/pipelines/controller/config/application.yaml{target="_blank"}) file for more file path configs based on different file systems

### Advanced Configurations

- `incrementalSchedule`: The schedule for automatic incremental pipeline runs.
  Uses the Spring CronExpression format, i.e., "second minute hour
  day-of-the-month month day-of-the-week"
  - "0 0 \* \* \* \*" means top of every hour
  - "_/40 _ \* \* \* \*" means every 40 seconds

!!! info "Info"

    Scheduling very frequent runs is resource intensive

- `purgeSchedule`: The schedule for automatic DWH snapshot purging. There is no
  benefit to scheduling the purge job more frequently than incremental runs.
  Uses the Spring CronExpression format.

- `numOfDwhSnapshotsToRetain`: The number of DWH snapshots to retain when the
  purge job runs. This parameter **must be > 0** or the purge job will not run.
  If a pipeline run fails for any reason, any partial output must be manually
  removed.

- `resourceList`: The comma-separated list of FHIR resources to fetch/monitor.
  Equivalent to pipeline `resourceList` parameter. Note there is no
  Questionnaire in our test FHIR server, read more
  [here](https://github.com/google/fhir-data-pipes/issues/785). Default:
  `Patient, Encounter, Observation, Questionnaire, Condition, Practitioner, Location, Organization`

- `numThreads`: The parallelism to be used for a pipeline job. In case of
  FlinkRunner, if the value is set to -1, then in the local execution mode the
  number of threads the job uses will be equal to the number of cores in the
  machine, whereas in the remote mode (cluster) only 1 thread is used. If set to
  a positive value, then in both modes the pipeline will use that many threads
  combined across all the workers.

- `autoGenerateFlinkConfiguration`: In case of Flink local execution mode (which
  is the default), generate Flink configuration file `flink-conf.yaml`
  automatically based on the parallelism set via the parameter `numThreads` and
  the `cores` available in the machine. The generated file will have the
  parameters set with optimised values necessary to run the pipelines without
  fail. Disable this parameter to manually pass the configuration file by
  pointing the environment variable FLINK_CONF_DIR to the directory where the
  flink-conf.yaml is placed.

!!! info "Info"

    For Flink non-local execution mode, this parameter has to be disabled and the configuration file has to be passed manually which has more fine-grained control parameters

- `createHiveResourceTables`: Boolean determining if resource tables should be
  automatically created on a Hive/Spark server. Primarily meant for
  single-machine deployment.

- `thriftserverHiveConfig`: Path to a file with the settings used to create
  tables. Required if createHiveResourceTables is `true`.

- `hiveResourceViewsDir`: Path to a directory containing
  [View Definition](concepts/views.md#viewdefinition-resource) for each resource
  type. If not set or set to empty string, automatic view creation is disabled.
  Otherwise, for each resource type, its view definition SQL queries are read
  and applied from corresponding files, i.e., any file that starts with the
  resource name and ends in `.sql`, e.g., `DiagnosticReport_flat.sql`. Only
  applies when createHiveResourceTables is `true`.

!!! info "Developers Note:"

    If you symlink `[repo_root]/docker/config/views` here you can use those predefined views in your dev environment too

- `viewDefinitionsDir`: The location from which
  [View Definition](concepts/views.md#viewdefinition-resource) resources are
  read and applied to the corresponding input FHIR resources. Any file in this
  directory that ends in `.json` is assumed to be a single ViewDefinition. To
  output these views to a relational database, the next `sinkDbConfigPath`
  should also be set.

- `structureDefinitionsPath`: Directory path containing the structure definition
  files for any custom profiles that need to be supported. If this starts with
  `classpath:` then a classpath resource is assumed and the path should always
  start with a `/`. Do not configure anything if custom profiles are not needed.
  Examples can be found below:

  - "config/r4-us-core-definitions"
  - "classpath:/r4-us-core-definitions"

- `fhirVersion`: # The fhir version to be used for the FHIR Context APIs. This
  is an enum value and the possible values should be one from the list mentioned
  [here](https://github.com/hapifhir/hapi-fhir/blob/a5eddc3837c72e5a6df4fd65150c8e10ec966f4e/hapi-fhir-base/src/main/java/ca/uhn/fhir/context/FhirVersionEnum.java#L37).
  Currently, only enums `R4` and `DSTU3` are supported by the application.

- `rowGroupSizeForParquetFiles`: This is the size of the Parquet Row Group (a
  logical horizontal partitioning into rows) that will be used for creating row
  groups in parquet file by pipelines. A large value means more data for one
  column can be fit into one big column chunk which will speed up the reading of
  column data. On the downside, more in-memory will be needed to hold the data
  before writing to files.

- `recursiveDepth`: The maximum depth for traversing StructureDefinitions in
  Parquet schema generation (if it is non-positive, the default 1 will be used).
  Note in most cases, the default 1 is sufficient and increasing that can result
  in significantly larger schema and more complexity. For details see
  [here](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#recursive-structures)

### FHIR Output Parameters

- `sinkDbConfigPath`: The configuration file for the sink database. If
  `viewDefinitionsDir` is set then the generated views are materialized and
  written to this DB. If not, the raw FHIR JSON resources are written to this
  DB. The default empty string disables this feature. Note enabling this feature
  can have a noticeable impact on pipelines performance.

- `sinkFhirServerUrl`: The base URL of the sink FHIR server. If not set, the
  feature for sending resources to a sink FHIR server is disabled.

- `sinkUserName`, `sinkPassword`: The following user-name/password should be set
  if the sink FHIR server supports Basic Auth. Default: `hapi`, `hapi123`
