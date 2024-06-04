# Running FHIR Data Pipes in a Docker Container
The repository includes a number of different docker compose configurations for a “Single Machine” Deployment. 

NOTE: These are examples to help you to get up and running quickly. The details of how one creates a distributed environment is beyond the scope of this document.

| Name | Description | Notes |
| ---- | ------------| ------|
| Basic "Single Machine" [Docker Compose Configuration](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-single.yaml) | Brings up the FHIR Pipelines Controller plus a Spark Thrift server, letting you more easily run Spark SQL queries on the Parquet files output by the Pipelines Controller. In this configuration the Spark master, one worker, and the Thrift server all run in the same container | Good for getting familiar with the controller and pipelines. Will build the image from source if it does not exist |
| “Single Machine” with the Released Image [Docker Compose Configuration ](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-released.yaml) | This is the same as compose-controller-spark-sql-single.yaml that it uses the released version of pipeline-controller docker image instead of building it from source. Useful for quick evaluation and demo environments | Note if local paths are used, they should start with `./ `or `../`. Also the mounted files should be readable by containers, e.g., world-readable |
| Single Machine "Local Cluster" [Docker Compose Configuration](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql.yaml) | While all Spark pieces are brought up on the same machine, this config can serve as an example of how to deploy Spark on a cluster environment as it uses different containers for the master, a worker and Thrift server | More complete configuration which shows different pieces that are needed for a cluster environment. See the readme for more details |

## Single Machine Deployment Docker Compose
The easiest way to get started is to set up a "Single machine deployment" using a docker image provided in the repository.

Using the [Basic Single Machine docker compose](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-
single.yaml) configuration this will:

*	Bring up the FHIR Pipelines Controller plus a Spark Thrift Server
* 	Start the Web Control Panel for interacting with the Controller for running full and incremental Pipelines and registering new views
* 	Generate Parquet files for each FHIR Resource in the /docker/dwh directory 
* 	Run the SQL-on-FHIR queries defined in /config/views/*.sql and register these via the Thrift Server
* 	Generate flattened views (defined in /config/views/*.json) in the database configured by sinkDbConfigPath (you will need to ensure the database is created)

Follow the [Single Machine Deployment tutorial](tutorial_single_machine/)

