# Relational DWH using custom "lossy" schema 

## Overview
In this tutorial you will learn how to configure and deploy FHIR Data Pipes to transform FHIR data into a Postgre Relational SQL data-warehouse using FHIR ViewDefinition Resources to define the custom _"lossy"_ schema.

## Requirements

*   A source [HAPI FHIR server](https://hapifhir.io/) configured to [use Postgres as its database](https://github.com/hapifhir/hapi-fhir-jpaserver-starter#postgresql-configuration)
    *   If you don't have a server, use a [local test server](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers) by following the instructions to bring up a source HAPI FHIR server with Postgres
*   [Docker](https://www.docker.com/)
    *   If you are using Linux, Docker must be in [sudoless mode](https://docs.docker.com/engine/install/linux-postinstall/)
*   [Docker Compose](https://docs.docker.com/compose/) - this guide assumes you are using the latest version 
*   The FHIR Data Pipes repository, cloned onto the host machine

## Configure the FHIR Pipelines Controller

Note: All file paths are relative to the root of the FHIR Data Pipes repository.

**NOTE: You need to configure only one of the following options:**

1. For FHIR Search API (works for any FHIR server): 
   * Open [`docker/config/application.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/config/application.yaml) and edit the value of `fhirServerUrl` to match the FHIR server you are connecting to. 
   * Comment out the `dbConfig` in this case.

2. For direct DB access (specific to HAPI FHIR servers):
   * Comment out `fhirServerUrl`
   * Set `dbConfig` to the DB connection config file, e.g., [`docker/config/hapi-postgres-config_local.json`](https://github.com/google/fhir-data-pipes/blob/master/docker/config/hapi-postgres-config_local.json); 
   * Edit the values in this file to match the database for the FHIR server you are connecting to.

## Set the sinkDbConfigPath
The sinkDb refers to the target database that will become the data warehouse.

With the default config, you will create both Parquet files (under `dwhRootPrefix`) and flattened views in the database configured by `sinkDbConfigPath` [here](https://github.com/google/fhir-data-pipes/blob/27d691e91d0fe6ef4c9624acba4e68bca145c973/docker/config/application.yaml#L42). 

Make sure to create the database referenced in the connection config file (default is a postgreSQL db named 'views'). You can do this with the following SQL query:

```sql
CREATE DATABASE views;
```
which you can run in Postgres like this:
```shell
PGPASSWORD=admin psql -h 127.0.0.1 -p 5432 -U admin postgres -c "CREATE DATABASE views"
```

For documentation of all config parameters, see [here](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/application.yaml).

If you are using the [local test servers](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers), things should work with the default values. If not, use the IP address of the Docker default bridge network. To find it, run the following command and use the "Gateway" value:

```
docker network inspect bridge | grep Gateway
```

The Single Machine docker configuration uses two environment variables, `DWH_ROOT` and `PIPELINE_CONFIG`, whose default values are defined in the [.env](https://github.com/google/fhir-data-pipes/blob/master/docker/.env) file. _To override them_, set the variable before running the `docker-compose` command. For example, to override the `DWH_ROOT` environment variable, run the following:

```
DWH_ROOT="$(pwd)/<path_to_dwh_directory>" docker compose -f docker/compose-controller-spark-sql-single.yaml up --force-recreate 
```

## Run the Single Machine configuration
To bring up the [`docker/compose-controller-spark-sql-single.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-single.yaml) configuration for the first time or if you have run this container in the past and want to include new changes pulled into the repo, run:

```
docker compose -f docker/compose-controller-spark-sql-single.yaml up --force-recreate --build
```

Alternatively, to run without rebuilding use:

```
docker compose -f docker/compose-controller-spark-sql-single.yaml up --force-recreate
```

Alternatively, [`docker/compose-controller-spark-sql.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql.yaml) serves as a very simple example on how to integrate the Parquet output of Pipelines in a Spark cluster environment.

Once started, the Pipelines Controller is available at `http://localhost:8090` and the Spark Thrift server is at `http://localhost:10001`.

The first time you run the Pipelines Controller, you must manually start a Full Pipeline run. In a browser go to `http://localhost:8090` and click the **Run Full** button. 

After running the Full Pipeline, use the Incremental Pipeline to update the Parquet files and tables. By default it is scheduled to run every hour, or you can manually trigger it.

If the Incremental Pipeline does not work, or you see errors like:

```
ERROR o.openmrs.analytics.PipelineManager o.openmrs.analytics.PipelineManager$PipelineThread.run:343 - exception while running pipeline: 
pipeline-controller    | java.sql.SQLException: org.apache.hive.service.cli.HiveSQLException: Error running query: org.apache.spark.sql.AnalysisException: Unable to infer schema for Parquet. It must be specified manually.
```

try running `sudo chmod -R 755` on the Parquet file directory, by default located at `docker/dwh`.

## Explore the resulting schema in PostgreSQL

Connect to the PostgreSQL RDBMS via docker using the cmd: `docker exec -it <container_name_or_id> bash`

If using the default container (hapi-fhir-db) run: `docker exec -it hapi_fhir_db bash`

Using psql connect to the 'views'  database: `psql -U admin -d views`

To list the tables: `\d`

| Schema |          Name           | Type  | Owner| 
 ---     | ----                    |---    | ---
| public | condition_flat          | table | admin|
| public | diagnostic_report_flat  | table | admin|
| public | immunization_flat       | table | admin|
| public | location_flat           | table | admin|
| public | medication_request_flat | table | admin|
| public | observation_flat        | table | admin|
| public | organization_flat       | table | admin|
| public | practitioner_flat       | table | admin|
| public | practitioner_role_flat  | table | admin|
| public | procedure_flat          | table | admin|


## Querying the database

Let's do some basic quality checks to make sure the data is uploaded properly (note
table names are case insensitive).

NOTES:

*   This assumes that the data loaded is from [this synthetic data set](https://github.com/google/fhir-data-pipes/synthea-hiv/sample_data)
*   You will see that the number of patients and observations is higher than the count in the FHIR Server. This is due to the flattening

```sql
SELECT COUNT(0) FROM patient_flat;
```
We should have exactly 114 patients:
```
+-----------+
| count     |
+-----------+
| 114       |
+-----------+
```

Doing the same for observations:
```sql
SELECT COUNT(0) FROM observation_flat;
```
```
+-----------+
| count  |
+-----------+
| 18343     |
+-----------+
```

## What's next
Now that you have
