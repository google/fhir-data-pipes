# FHIR Pipelines Controller

The FHIR Pipelines Controller helps you schedule and manage the continuous
transformation of data from a [HAPI FHIR server](https://hapifhir.io/) to a
collection of Apache Parquet files. It uses the FHIR Data Pipes pipeline to run
either full or incremental transformations to a Parquet data warehouse.

The JDBC mode of FHIR Pipelines Controller only works with HAPI FHIR servers.
You can see
[an example of configuring a HAPI FHIR server to use Postgres here](https://github.com/hapifhir/hapi-fhir-jpaserver-starter#postgresql-configuration).

## Usage

### Setup

1.  Clone the
    [fhir-data-pipes GitHub repository](https://github.com/google/fhir-data-pipes),
    open a terminal window.
2.  `cd` to the directory where you cloned it.
3.  Change to the `controller` directory: `cd pipelines/controller/`.

Later terminal commands will assume your working directory is the `controller`
directory.

Next, configure the FHIR Pipelines Controller. The FHIR Pipelines Controller
relies on several configuration files to run. Edit them to match your
environment and requirements.

- [`pipelines/controller/config/application.yaml`](config/application.yaml)
  - The main settings for the FHIR Pipelines Controller. Edit the values to
    match your HAPI FHIR server and use case.
- [`pipelines/controller/config/hapi-postgres-config.json`](config/hapi-postgres-config.json)
  - JDBC settings. Edit the values to match the Postgres database of your HAPI
    FHIR server.
- [`pipelines/controller/config/flink-conf.yaml`](config/flink-conf.yaml)
  - Customized FlinkRunner settings. To use this file, set the
    [`FLINK_CONF_DIR` environmental variable](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/):
    `export FLINK_CONF_DIR=[PATH]/flink-conf.yaml`. Changing
    `taskmanager.memory.network.max` is necessary to avoid memory errors for
    large datasets or when running on machines with limited memory.
- [`pipelines/controller/config/thriftserver-hive-config.json`](config/thriftserver-hive-config.json)
  - Settings used to connect to a Spark server when `createHiveResourceTables`
    is set to `true`. Edit the values to match your Spark server if necessary.

### Run the FHIR Pipelines Controller

There are 2 ways to run the FHIR Pipelines Controller.

Using Spring Boot:

```
mvn spring-boot:run
```

Running the JAR directly:

```
mvn clean install
java -jar ./target/controller-bundled.jar
```

After running, open a web browser and visit http://localhost:8080. You should
see the FHIR Pipelines Control Panel.

There are 3 ways to have the FHIR Pipelines Controller run the transformation
pipeline:

- Manually trigger the **Run Full** option by clicking the button. This
  transforms all of the selected FHIR resource types to Parquet files. You are
  required to use the **Run Full** option once before using any of the following
  incremental options.
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
