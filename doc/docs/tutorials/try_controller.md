# Try the Pipelines Controller with HAPI FHIR Store

The FHIR Pipelines Controller makes it easy to schedule and manage the transformation of data from a [HAPI FHIR](https://hapifhir.io/) server to a collection of Apache Parquet files. It uses FHIR Data Pipes JDBC pipeline to run either full or incremental transformations to a Parquet data warehouse. 

The FHIR Pipelines Controller only works with HAPI FHIR servers using Postgres. You can see [an example of configuring a HAPI FHIR server to use Postgres here](https://github.com/hapifhir/hapi-fhir-jpaserver-starter#postgresql-configuration). 

This guide will show you how to set up the FHIR Pipelines Controller with a test HAPI FHIR server. It assumes you are using Linux, but should work with other environments with minor adjustments.

## Clone the fhir-data-pipes repository

Clone the [fhir-data-pipes GitHub repository](https://github.com/google/fhir-data-pipes) using your preferred method. After cloned, open a terminal window and `cd` to the directory where you cloned it. Later terminal commands will assume your working directory is the repository root.


## Set up the test server

The repository includes a [Docker Compose](https://docs.docker.com/compose/) configuration to bring up a HAPI FHIR server configured to use Postgres.

To set up the test server, [follow these instructions](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers). At step two, follow the instructions for "HAPI source server with Postgres".

## Configure the FHIR Pipelines Controller

First, open [`pipelines/controller/config/application.yml`](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/application.yaml) in a text editor.

Change fhirServerUrl to be:


```
fhirServerUrl: "http://localhost:8091/fhir"
```

Read through the rest of the file to see other settings. The other lines may remain the same. Note the value of `dwhRootPrefix`, as it will be where the Parquet files are written. You can also adjust this value if desired. Save and close the file.

Next, open [`pipelines/controller/config/hapi-postgres-config.json`](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/hapi-postgres-config.json) in a text editor.

Change `databaseHostName` to be:

```
"databaseHostName" : "localhost"
```

Save and close the file.

## Run the FHIR Pipelines Controller

From the terminal run:

```shell
cd pipelines/controller/
mvn spring-boot:run
```

Open a web browser and visit http://localhost:8080. You should see the FHIR Pipelines Control Panel.

![Pipelines Control Panel](../images/pipelines_control_panel.png)

Before automatic incremental runs can occur, you must manually trigger a full run. Under the **Run Full Pipeline** section, click on **Run Full**. Wait for the run to complete. 

## Explore the configuration settings

The Control Panel shows the options being used by the FHIR Pipelines Controller.

### Main configuration parameters

This section corresponds to the settings in the `application.yml` file.

### Batch pipeline non-default configurations

This section calls out FHIR Data Pipes batch pipeline settings that are different from their default values. These are also mostly derived from `application.yml`. Use these settings if you want to run the batch pipeline manually.

## Query the DWH

On your machine, look for the Parquet files created in the directory specified by `dwhRootPrefix` in the application.yml file. You can explore the data using a Jupyter notebook with pyspark and pandas libraries installed

Alternatively you can load the Parquet into an SQL Query Engine like SparkSQL. See the tutorials section for more.
