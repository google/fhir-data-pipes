# Single machine deployment tutorial

The repository includes a "Single Machine"
[Docker Compose](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql.yaml)
configuration which brings up the FHIR Pipelines Controller plus a Spark Thrift
server, letting you more easily run Spark SQL queries on the Parquet files
output by the Pipelines Controller.

To learn how the Pipelines Controller works on its own, try out the
[FHIR Pipelines Controller](https://github.com/google/fhir-data-pipes/wiki/Try-out-the-FHIR-Pipelines-Controller).

## Requirements

- A source [HAPI FHIR server](https://hapifhir.io/) configured to
  [use Postgres as its database](https://github.com/hapifhir/hapi-fhir-jpaserver-starter#postgresql-configuration)
  - If you don't have a server, use a
    [local test server](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers)
    by following the instructions to bring up a source HAPI FHIR server with
    Postgres
- [Docker](https://www.docker.com/)
  - If you are using Linux, Docker must be in
    [sudoless mode](https://docs.docker.com/engine/install/linux-postinstall/)
- [Docker Compose](https://docs.docker.com/compose/) - this guide assumes you
  are using the latest version
- The FHIR Data Pipes repository, cloned onto the host machine

## Configure the FHIR Pipelines Controller

All file paths are relative to the root directory of the FHIR Data Pipes
repository.

**NOTE: You need to configure only one of the following options:**

=== "For FHIR Search API (works for any FHIR server):"

    1. Open [`docker/config/application.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/config/application.yaml) and edit the parameter `fhirServerUrl` to match the FHIR server you are connecting to.
    2. Comment out the paramter: `dbConfig`.

=== "For direct DB access (specific to HAPI FHIR servers):"

    1. Open [`docker/config/application.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/config/application.yaml) and comment out the paramter: `fhirServerUrl`
    2. Set `dbConfig` to the DB connection config file, e.g. [`docker/config/hapi-postgres-config_local.json`](https://github.com/google/fhir-data-pipes/blob/master/docker/config/hapi-postgres-config_local.json).
    3. Edit the json values in this file to match the database for the FHIR server you are connecting to.

### Parquet Files and Flattened Views

FHIR Data Pipes supports the option to transform FHIR data into an RDBMS (as the
data-warehouse) using
[FHIR View Definition](../concepts/views.md#viewdefinition-resource) resources
to define the custom schema. This option in our tutorial will be specific to
PostgreSQL, but this feature is compatible with any RDBMS.

The concept of "flattened views" comes from applying ViewDefinitions to FHIR
Resources to get flat tables. Please check
[Schema and Flat Views](../concepts/views.md#output-data-formats) to learn more.

With the default config, you will create both Parquet files ( under
`dwhRootPrefix`) and flattened views in the relational database configured by
`sinkDbConfigPath`
[here](https://github.com/google/fhir-data-pipes/blob/27d691e91d0fe6ef4c9624acba4e68bca145c973/docker/config/application.yaml#L42)
(The `sinkDbConfigPath` refers to the target DB that will become the
data-warehouse)

**If you do not want flat tables in a sink DB, comment out that parameter!**

If you do need them, make sure you create the DB referenced in the connection
config file with the following SQL query:

```sql
CREATE DATABASE views;
```

Run this query in Postgres:

```shell
PGPASSWORD=admin psql -h 127.0.0.1 -p 5432 -U admin postgres -c "CREATE DATABASE views"
```

For documentation of all config parameters, see
[here](https://github.com/google/fhir-data-pipes/blob/master/pipelines/controller/config/application.yaml).

**Note:** If you are using the
[local test servers](https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers),
things should work with the default values. If not, use the IP address of the
Docker default bridge network. To find it, run the following command and use the
"Gateway" value:

```
docker network inspect bridge | grep Gateway
```

<br>

The Single Machine docker configuration uses two environment variables,
`DWH_ROOT` and `PIPELINE_CONFIG`, whose default values are defined in the
[.env](https://github.com/google/fhir-data-pipes/blob/master/docker/.env) file.

**_To override them:_** set the desired variable before running the
`docker-compose` command. For example, to override the `DWH_ROOT` environment
variable, run the following:

```
DWH_ROOT="$(pwd)/<path_to_dwh_directory>" docker compose -f docker/compose-controller-spark-sql-single.yaml up --force-recreate
```

## Run the Single Machine Configuration

To bring up the
[`docker/compose-controller-spark-sql-single.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-single.yaml)
configuration for the first time or if you have run this container in the past
and want to include new changes pulled into the repo, run:

```
docker compose -f docker/compose-controller-spark-sql-single.yaml up --force-recreate --build
```

Alternatively, to run without rebuilding use:

```
docker compose -f docker/compose-controller-spark-sql-single.yaml up --force-recreate
```

Alternatively,
[`docker/compose-controller-spark-sql.yaml`](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql.yaml)
serves as a very simple example on how to integrate the Parquet output of
Pipelines in a Spark cluster environment.

Once started, the Pipelines Controller is available at `http://localhost:8090`
and the Spark Thrift server is at `http://localhost:10001`.

The first time you run the Pipelines Controller, you are **required** to start a
Full Pipeline run. In a browser go to `http://localhost:8090` and click the
**Run Full** button.

After running the Full Pipeline, use the Incremental Pipeline to update the
Parquet files and tables. By default, it is scheduled to run every hour, or you
can manually trigger it.

<br>

If the Incremental Pipeline does not work, or you see errors like:

```
ERROR o.openmrs.analytics.PipelineManager o.openmrs.analytics.PipelineManager$PipelineThread.run:343 - exception while running pipeline:
pipeline-controller    | java.sql.SQLException: org.apache.hive.service.cli.HiveSQLException: Error running query: org.apache.spark.sql.AnalysisException: Unable to infer schema for Parquet. It must be specified manually.
```

Try running `sudo chmod -R 755` on the Parquet file directory, by default
located at `docker/dwh`.

## Explore the resulting schema in PostgreSQL

If you set the `sinkDbConfigPath` parameter to output views to a PostgreSQL DWH,
connect to the DB via docker using this cmd:
`docker exec -it <container_name_or_id> bash`

If using the default container (hapi-fhir-db) run:
`docker exec -it hapi_fhir_db bash`

Using psql connect to the 'views' database: `psql -U admin -d views`

To list the tables: `\d`. It should look something like this:

| Schema | Name                                                                                                                            | Type  | Owner |
| ------ | ------------------------------------------------------------------------------------------------------------------------------- | ----- | ----- |
| public | [condition_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Condition_flat.sql)                  | table | admin |
| public | [diagnostic_report_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/DiagnosticReport_flat.sql)   | table | admin |
| public | [immunization_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Immunization_flat.sql)            | table | admin |
| public | [location_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Location_flat.sql)                    | table | admin |
| public | [medication_request_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/MedicationRequest_flat.sql) | table | admin |
| public | [observation_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Observation_flat.sql)              | table | admin |
| public | [organization_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Organization_flat.sql)            | table | admin |
| public | [practitioner_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Practitioner_flat.sql)            | table | admin |
| public | [practitioner_role_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/PractitionerRole_flat.sql)   | table | admin |
| public | [procedure_flat](https://github.com/google/fhir-data-pipes/blob/master/docker/config/views/Procedure_flat.sql)                  | table | admin |

## View and Analyze the Data

Let's do some basic quality checks to make sure the data is uploaded properly
(note table names are case-insensitive).

=== "Spark Thrift Server"

    Connect to the Spark Thrift server using a client that supports Apache Hive.
    For example, if using the JDBC driver, the URL should be `jdbc:hive2://localhost:10001`.

    The pipeline will automatically create `Patient`, `Encounter`, and `Observation` tables when run.


    ```hiveql
    SELECT COUNT(0) FROM Patient;
    ```
    We should have exactly 79 patients:
    ```
    +-----------+
    | count(0)  |
    +-----------+
    | 79        |
    +-----------+
    ```

    Doing the same for observations:
    ```hiveql
    SELECT COUNT(0) FROM Observation;
    ```
    ```
    +-----------+
    | count(0)  |
    +-----------+
    | 17279     |
    +-----------+
    ```

=== "Sink Database"

    Connect to your sink DB with the database client of your choice.

    **Note:** You will see that the number of patients and observations is higher than the count in the Thrift Server. This is due to the flattening

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

    Let's do the same for observations:
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

## What's Next

Now that the data is available in an SQL queryable format, you can start to
explore it using SQL or jupyter notebooks or build dashboards using common open
source tools like Apache SuperSet.
