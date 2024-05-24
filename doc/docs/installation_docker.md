# FHIR Data Pipes in a Docker Container


The easiest way to get started is to set up a [single machine deployment] using a docker image provided in the repository.

By default we use the Basic "Single Machine"  [Docker Compose Configuration](https://github.com/google/fhir-data-pipes/blob/master/docker/compose-controller-spark-sql-
single.yaml). For descriptions of the different available compose configurations see [here](link_to_section)

By default, the Single Machine docker compose configuration will:

*	Bring up the FHIR Pipelines Controller plus a Spark Thrift Server
* 	Start the Web Control Panel for interacting with the Controller for running full and incremental Pipelines and registering new views
* 	Generate Parquet files for in the /docker/dwh directory 
* 	Run the SQL-on-FHIR queries defined in /config/views/*.sql and register these via the Thrift Server
* 	Generate flattened views (defined in /config/views/*.json) in the database configured by sinkDbConfigPath (you will need to ensure the database is created)

The rest of this guide covers setting up and deploying an end-to-end environment for running analytics queries on FHIR Data. It includes optional instructions for provisioning a FHIR Server with sample data.


### Requirements

* 	Docker - If you are using Linux, Docker must be in sudoless mode
* 	Docker Compose
* 	The FHIR Data Pipes repository, cloned onto the host machine
