# FHIR Pipelines Controller

The FHIR Pipelines Controller helps you schedule and manage the transformation
of data from a [HAPI FHIR server](https://hapifhir.io/) to a collection of
Apache Parquet files. It uses the FHIR Data Pipes JDBC pipeline to run either
full or incremental transformations to a Parquet data warehouse.

The FHIR Pipelines Controller only works with HAPI FHIR servers using Postgres.
You can see
[an example of configuring a HAPI FHIR server to use Postgres here](https://github.com/hapifhir/hapi-fhir-jpaserver-starter#postgresql-configuration).

## Usage

### Setup

Clone the
[fhir-data-pipes GitHub repository](https://github.com/google/fhir-data-pipes),
open a terminal window, and `cd` to the directory where you cloned it. Later
terminal commands will assume your working directory is the repository root.

Next, configure the FHIR Pipelines Controller. The FHIR Pipelines Controller
relies on several configuration files to run. Edit them to match your
environment and requirements.

- <code>[pipelines/controller/config/application.yaml](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/application.yaml)</code>
  - The main settings for the FHIR Pipelines Controller. Edit the values to
    match your HAPI FHIR server and use case.
- <code>[pipelines/controller/config/hapi-postgres-config.json](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/hapi-postgres-config.json)</code>
  - JDBC settings. Edit the values to match the Postgres database of your HAPI
    FHIR server.
- <code>[pipelines/controller/config/flink-conf.yaml](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/flink-conf.yaml)</code>
  - FlinkRunner settings. Edit the values to adjust Flink, if desired.
- <code>[pipelines/controller/config/thriftserver-hive-config.json](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/thriftserver-hive-config.json)</code>
  - Settings used to connect to a Spark server when
    <code>createHiveResourceTables</code> is set to <code>true</code>. Edit the
    values to match your Spark server if necessary.

### Run the FHIR Pipelines Controller

From the terminal run:

```
cd pipelines/controller/
mvn spring-boot:run
```

Open a web browser and visit http://localhost:8080. You should see the FHIR
Pipelines Control Panel.

There are 3 ways to have the FHIR Pipelines Controller run the transformation
pipeline:

- Manually trigger the **Run Full** option by clicking the button. This
  transforms the whole database to Parquet files. You are required to use the
  **Run Full** option once before using any of the following incremental
  options.
- Manually trigger the **Run Incremental** option by clicking the button. This
  only outputs resources that are new or changed since the last run.
- Automatically scheduled incremental runs, as specified by
  `incrementalSchedule` in the `application.yaml` file. You can see when the
  next scheduled run is near the top of the control panel.

After running the pipeline, look for the Parquet files created in the directory
specified by `dwhRootPrefix` in the `application.yaml` file.

### Explore the configuration settings

The bottom area of the control panel shows the options being used by the FHIR
Pipelines Controller.

#### Main configuration parameters

This section corresponds to the settings in the `application.yml` file.

#### Batch pipeline non-default configurations

This section calls out FHIR Data Pipes batch pipeline settings that are
different from their default values. These are also mostly derived from
`application.yml`. Use these settings if you want to run the batch pipeline
manually.
