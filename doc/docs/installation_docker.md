# FHIR Data Pipes Docker Image
A “Single Machine” with Released Image [Docker Compose Configuration ](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-released.yaml) is maintained.

This docker-compose configuration is for bringing up a pipeline controller along with a single-process Spark environment with a JDBC endpoint. This is particularly useful for quick evaluation and demo environments.

```
* If local paths are used, they should start with `./ `or `../`. 
* The mounted files should be readable by containers, e.g., world-readable
```

Using the "Single Machine" configuration you will be able to quickly and easily:

*   Bring up the FHIR Pipelines Controller plus a Spark Thrift Server
* 	Start the Web Control Panel for interacting with the Controller for running full and incremental Pipelines and registering new views
* 	Generate Parquet files for each FHIR Resource in the /docker/dwh directory 
* 	Run the SQL-on-FHIR queries defined in /config/views/*.sql and register these via the Thrift Server
* 	Generate flattened views (defined in /config/views/*.json) in the database configured by sinkDbConfigPath (you will need to ensure the database is created)

!!! tip

    The easiest way to get up and running quickly is to follow the [Single Machine Deployment tutorial](../tutorials/single_machine/).

## Sample Docker Configurations
The repository includes a number of other sample "Single Machine" docker compose configurations. These provide samples of different deployment configurations.

| Name | Description | Notes |
| ---- | ------------| ------|
| Basic "Single Machine" [Docker Compose Configuration](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-single.yaml) | Brings up the FHIR Pipelines Controller plus a Spark Thrift server, letting you more easily run Spark SQL queries on the Parquet files output by the Pipelines Controller. In this configuration the Spark master, one worker, and the Thrift server all run in the same container | Good for getting familiar with the controller and pipelines. Will build the image from source if it does not exist |
| Single Machine "Local Cluster" [Docker Compose Configuration](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql.yaml) | While all Spark pieces are brought up on the same machine, this config can serve as an example of how to deploy Spark on a cluster environment as it uses different containers for the master, a worker and Thrift server | More complete configuration which shows different pieces that are needed for a cluster environment. See the readme for more details |

