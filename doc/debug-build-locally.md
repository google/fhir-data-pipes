# Running E2E Tests Locally with Docker

This document provides a lightweight, repeatable workflow for running the E2E
pipeline tests locally so you can iterate quickly without waiting for a CI run.

**Note:** Depending on what stage of the build is failing for your PR, you can
bring up only the services relevant to that stage.

## Prerequisites

- Docker installed (including `docker compose`); one way of achieving this is to
  install Docker Desktop.
- A local JDK 17 + Maven 3.8.x for the Maven build steps.
- The GitHub repo is checked out locally.

## Environment setup

1. Create an external Docker network named `cloudbuild`. This is required
   because the Docker Compose files in this project reference it as an external
   network:

```bash
docker network create cloudbuild
```

2. From the repo root, export image-tag variables used in the build and run
   commands below:

```bash
export _TAG=local
export _REPOSITORY=fhir-analytics
```

## Initial Setup

### 1. Start shared services

Start the HAPI source server and both sink servers:

```bash
docker compose -f docker/hapi-compose.yml -p hapi-compose up --force-recreate --remove-orphans -d

SINK_SERVER_NAME=sink-server-search \
SINK_SERVER_PORT=9001 \
docker compose \
  -f docker/sink-compose.yml \
  -p sink-server-search \
  up --force-recreate --remove-orphans -d

SINK_SERVER_NAME=sink-server-jdbc \
SINK_SERVER_PORT=9002 \
docker compose \
  -f docker/sink-compose.yml \
  -p sink-server-jdbc \
  up --force-recreate --remove-orphans -d
```

Wait for all three servers to finish initializing before continuing. You can use
the included readiness script:

```bash
./e2e-tests/wait_for_start.sh \
  --HAPI_SERVER_URLS=http://localhost:8091,http://localhost:9001,http://localhost:9002
```

Once it exits successfully you should see output like:

```
FHIR SERVER http://localhost:8091 STARTED SUCCESSFULLY
FHIR SERVER http://localhost:9001 STARTED SUCCESSFULLY
FHIR SERVER http://localhost:9002 STARTED SUCCESSFULLY
```

You can also verify all four containers (1 postgres + 3 HAPI) are running:

```bash
docker ps
```

Expected output:

```
CONTAINER ID   IMAGE                     COMMAND                  CREATED              STATUS              PORTS                    NAMES
3a23504297d0   hapiproject/hapi:latest   "java --class-path /…"   About a minute ago   Up About a minute   0.0.0.0:9002->8080/tcp   sink-server-jdbc
c13ad0618e65   hapiproject/hapi:latest   "java --class-path /…"   2 minutes ago        Up 2 minutes        0.0.0.0:9001->8080/tcp   sink-server-search
1f301480d87f   hapiproject/hapi:latest   "java --class-path /…"   15 minutes ago       Up 15 minutes       0.0.0.0:8091->8080/tcp   hapi-server
815bc05a6005   postgres                  "docker-entrypoint.s…"   15 minutes ago       Up 15 minutes       0.0.0.0:5432->5432/tcp   hapi-fhir-db
```

### 2. Compile the Java modules

```bash
mvn --no-transfer-progress -e -T 2C install -Dlicense.skip=true -Dspotless.apply.skip=true
```

### 3. Upload sample synthetic data to HAPI

Upload sample synthetic data stored in
[sample_data](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/sample_data)
to the HAPI source server using the
[Synthea data uploader](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/README.md#Uploader).

The uploader requires that you install the `uploader` module requirements.

It is a good idea to first create a Python `virtualenv` before running the
installation command to avoid conflicts with other Python packages you may have
installed globally.

If you use `virtualenv` then you can do it by running:

```bash
$ virtualenv -p python3 venv
$ source ./venv/bin/activate
```

or if you use the python standard library `venv` module, you can do it by
running:

```bash
$ python3 -m venv venv
$ source ./venv/bin/activate
```

Then, you can install the requirements with:

```bash
pip3 install -r ./synthea-hiv/uploader/requirements.txt
```

Run the uploader script to upload the synthetic data to the HAPI source server
brought up in step 1 (note: the source server is on port **8091**):

```bash
python3 ./synthea-hiv/uploader/main.py HAPI http://localhost:8091/fhir \
--input_dir ./synthea-hiv/sample_data --cores 8
```

Depending on your machine, using too many cores may slow down your machine or
cause JDBC connection pool errors with the HAPI FHIR server. Reducing the number
of cores using the `--cores` flag should help at the cost of increasing the time
to upload the data.

### 4. Build and run the batch pipeline

**Note:** this step is only needed if the E2E is failing in the batch pipeline
step; otherwise skip.

Build the pipeline image:

```bash
cd pipelines/batch
docker build -t ${_REPOSITORY}/batch-pipeline:${_TAG} .
cd ../..
```

Run the FHIR-search job against the HAPI source server (port **8091**):

```bash
docker run --rm \
  -e FHIR_SERVER_URL=http://localhost:8091/fhir \
  -e PARQUET_PATH=/workspace/e2e-tests/FHIR_SEARCH_HAPI \
  -e OUTPUT_PARQUET_VIEW_PATH=/workspace/e2e-tests/FHIR_SEARCH_HAPI/VIEWS_TIMESTAMP_1 \
  -e SINK_PATH=http://localhost:9001/fhir \
  -e SINK_USERNAME=hapi -e SINK_PASSWORD=hapi \
  -v $(pwd):/workspace \
  --network host  \
  ${_REPOSITORY}/batch-pipeline:${_TAG}
```

**Note:** Running may overwrite previous data in the PARQUET_PATH and
OUTPUT_PARQUET_VIEW_PATH folders.

### 5. Run e2e tests

**Note:** Only run this step if the E2E tests for this stage are failing;
otherwise skip.

Execute the HAPI search test directly from the repo:

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ FHIR_SEARCH_HAPI FHIR_SEARCH_HAPI_JSON http://localhost:9001
```

## JDBC Mode

**Note:** Only run this section if debugging JDBC mode; otherwise skip.

### 1. Run batch pipeline for JDBC mode

Run the JDBC mode job against HAPI:

```bash
docker run --rm \
  -e JDBC_MODE_ENABLED=true \
  -e JDBC_MODE_HAPI=true \
  -e FHIR_SERVER_URL=http://localhost:8091/fhir \
  -e SINK_PATH=http://localhost:9002/fhir \
  -e SINK_USERNAME=hapi -e SINK_PASSWORD=hapi \
  -e FHIR_DATABASE_CONFIG_PATH=/workspace/utils/hapi-postgres-config.json \
  -e PARQUET_PATH=/workspace/e2e-tests/JDBC_HAPI \
  -e OUTPUT_PARQUET_VIEW_PATH=/workspace/e2e-tests/JDBC_HAPI/VIEWS_TIMESTAMP_1 \
  -e JDBC_FETCH_SIZE=1000 \
  -v $(pwd):/workspace \
  --network host \
  ${_REPOSITORY}/batch-pipeline:${_TAG}
```

### 2. Run e2e tests for JDBC mode

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ JDBC_HAPI JDBC_HAPI_FHIR_JSON http://localhost:9002
```

## Bulk Export Mode

**Note:** Only run this section if debugging Bulk Export mode; otherwise skip.

### 1. Run batch pipeline for Bulk Export mode

```bash
docker run --rm \
  -e FHIR_FETCH_MODE=BULK_EXPORT \
  -e FHIR_SERVER_URL=http://localhost:8091/fhir \
  -e PARQUET_PATH=/workspace/e2e-tests/BULK_EXPORT \
  -e OUTPUT_PARQUET_VIEW_PATH=/workspace/e2e-tests/BULK_EXPORT/VIEWS_TIMESTAMP_1 \
  -v $(pwd):/workspace \
  --network host \
  ${_REPOSITORY}/batch-pipeline:${_TAG}
```

### 2. Run e2e tests for Bulk Export mode

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ BULK_EXPORT BULK_EXPORT_FHIR_JSON NONE
```

## Controller and Spark Mode

**Note:** Only run this section if debugging Controller and Spark mode;
otherwise skip.

### 1. Turn down sink servers

```bash
docker compose -f docker/sink-compose.yml -p sink-server-search down -v
docker compose -f docker/sink-compose.yml -p sink-server-jdbc down -v
```

### 2. Create views database

The `views` database is used for creating flat views from ViewDefinitions.
It is created inside the `hapi-fhir-db` PostgreSQL container that was started
in the Initial Setup section as part of `hapi-compose.yml`. That same Postgres
instance hosts both the `hapi` database (used by the HAPI FHIR server) and this
`views` database (used by the pipeline controller).

```bash
docker run --rm --network host -e PGPASSWORD=admin postgres \
  psql -U admin -d postgres -h localhost -p 5432 -c 'CREATE DATABASE views;'
```

### 3. Build the controller/spark image

```bash
docker build -t ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG} -f e2e-tests/controller-spark/Dockerfile .
```

### 4. Bring up controller and Spark containers

Clear any Parquet output from previous runs first. The validation script globs
across all `controller_DWH_TIMESTAMP_*` subdirectories, so leftover directories
cause double-counted resource totals and test failures.

```bash
rm -rf e2e-tests/controller-spark/dwh/controller_DWH_TIMESTAMP_*

PIPELINE_CONFIG=$(pwd)/docker/config \
DWH_ROOT=$(pwd)/e2e-tests/controller-spark/dwh \
docker compose \
  -f docker/compose-controller-spark-sql-single.yaml \
  up --force-recreate -d
```

### 5. Run e2e test for controller and Spark

```bash
docker run --rm -v $(pwd):/workspace --network cloudbuild ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG}
```

### 6. Bring down controller and Spark containers

When you are done with debugging the controller-spark issues, you can bring
those containers down.

```bash
docker compose -f docker/compose-controller-spark-sql-single.yaml down -v
```

## FHIR to FHIR Sync Mode

**Note:** Only run this section if debugging FHIR to FHIR sync mode; otherwise
skip.

### 1. Launch HAPI FHIR Sink Server for controller

```bash
docker compose -f docker/sink-compose.yml -p sink-server down -v

SINK_SERVER_NAME=sink-server-controller \
SINK_SERVER_PORT=9001 \
docker compose \
  -f docker/sink-compose.yml \
  -p sink-server \
  up --force-recreate -d
```

### 2. Recreate the views database

The pipeline controller requires the `views` PostgreSQL database even in FHIR-to-FHIR
sync mode. If the HAPI source server was restarted with `-v` since the Controller and
Spark Mode section ran, the database will have been deleted and must be recreated. 
It is required because it is default when `FHIRDATA_SINKDBCONFIGPATH` is not set. So run:

```bash
docker run --rm --network host -e PGPASSWORD=admin postgres \
  psql -U admin -d postgres -h localhost -p 5432 -c 'CREATE DATABASE views;'
```

### 3. Bring up pipeline controller for FHIR to FHIR sync

**Note:** You can use the controller from the previous step if you have it
running

```bash
PIPELINE_CONFIG=$(pwd)/docker/config \
DWH_ROOT=$(pwd)/e2e-tests/controller-spark/dwh \
FHIRDATA_SINKFHIRSERVERURL=http://sink-server-controller:8080/fhir \
FHIRDATA_GENERATEPARQUETFILES=false \
FHIRDATA_CREATEHIVERESOURCETABLES=false \
FHIRDATA_CREATEPARQUETVIEWS=false \
FHIRDATA_SINKDBCONFIGPATH= \
FHIRDATA_NUMTHREADS=2 \
docker compose \
  -f docker/compose-controller-spark-sql-single.yaml \
  up --force-recreate --no-deps -d pipeline-controller
```

**Note:** `FHIRDATA_NUMTHREADS=2` caps Flink at 2 parallel tasks when running
locally. Without this, the pipeline spawns as many tasks as available CPUs,
which exhausts the HAPI client's HTTP connection pool (default `maxPerRoute=2`)
and causes `ConnectionPoolTimeoutException` errors. Setting it to 1 also fails
because the controller's Flink memory formula allocates only 32 network buffers
at parallelism=1 — too few for the pipeline's operator graph.

### 4. Run e2e test for FHIR to FHIR sync

```bash
docker run --rm -e DWH_TYPE=FHIR -v $(pwd):/workspace --network cloudbuild ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG}
```

### 5. Bring down pipeline controller and sink

```bash
docker compose -f docker/compose-controller-spark-sql-single.yaml down -v
docker compose -f docker/sink-compose.yml -p sink-server down -v
```

## OpenMRS Mode

**Note:** Only run this section if debugging OpenMRS mode; otherwise skip.

OpenMRS requires approximately 10 GB of JVM heap to start. Before launching it,
stop any containers from previous sections that are no longer needed to free up
memory:

```bash
# Stop controller/spark stack if still running
docker compose -f docker/compose-controller-spark-sql-single.yaml down -v

# Stop any leftover sink-server stacks
docker compose -f docker/sink-compose.yml -p sink-server down -v
docker compose -f docker/sink-compose.yml -p sink-server-search down -v
docker compose -f docker/sink-compose.yml -p sink-server-jdbc down -v

# Stop HAPI source server (not used in OpenMRS mode)
docker compose -f docker/hapi-compose.yml -p hapi-compose down
```

### 1. Launch OpenMRS Server and HAPI FHIR Sink Server for OpenMRS

```bash
SINK_SERVER_NAME=sink-server-for-openmrs \
SINK_SERVER_PORT=9002 \
docker compose \
  -f docker/openmrs-compose.yaml \
  -f docker/sink-compose.yml \
  -p openmrs-project \
  up --force-recreate --remove-orphans -d
```

Wait for OpenMRS to finish initializing before continuing — it typically takes
5–15 minutes. The readiness script polls every 60 seconds (up to 20 minutes):

```bash
./e2e-tests/wait_for_start.sh \
  --OPENMRS_SERVER_URLS=http://localhost:8099
```

Once it exits successfully you should see:

```
OPENMRS SERVER http://localhost:8099/openmrs/ws/fhir2/R4 STARTED SUCCESSFULLY
```

### 2. Upload to OpenMRS

```bash
python3 ./synthea-hiv/uploader/main.py OpenMRS \
http://localhost:8099/openmrs/ws/fhir2/R4 --convert_to_openmrs \
--input_dir ./synthea-hiv/sample_data --cores 1
```

This takes a while too, wait for the data import to complete.

### 3. Run batch pipeline for FHIR-search mode with OpenMRS source

```bash
docker run --rm \
  -e FHIR_SERVER_URL=http://localhost:8099/openmrs/ws/fhir2/R4 \
  -e PARQUET_PATH=/workspace/e2e-tests/FHIR_SEARCH_OPENMRS \
  -e OUTPUT_PARQUET_VIEW_PATH=/workspace/e2e-tests/FHIR_SEARCH_OPENMRS/VIEWS_TIMESTAMP_1 \
  -e SINK_PATH=http://localhost:9002/fhir \
  -e SINK_USERNAME=hapi -e SINK_PASSWORD=hapi \
  -v $(pwd):/workspace \
  --network host \
  ${_REPOSITORY}/batch-pipeline:${_TAG}
```

### 4. Run e2e test for FHIR-search mode with OpenMRS source

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ \
  FHIR_SEARCH_OPENMRS \
  FHIR_SEARCH_OPENMRS_JSON \
  http://localhost:9002 --openmrs
```

### 5. Reset the sink server

Both the FHIR-search and JDBC pipelines write to the same sink server. Reset
it before the JDBC run to avoid the sink accumulating resources from both
pipelines, which would cause step 6's count validation to fail.

```bash
docker compose -f docker/sink-compose.yml -p openmrs-project down -v

SINK_SERVER_NAME=sink-server-for-openmrs \
SINK_SERVER_PORT=9002 \
docker compose \
  -f docker/sink-compose.yml \
  -p openmrs-project \
  up --force-recreate -d
```

Wait for the sink to be ready:

```bash
./e2e-tests/wait_for_start.sh --HAPI_SERVER_URLS=http://localhost:9002
```

### 6. Run batch pipeline for JDBC mode with OpenMRS source

```bash
docker run --rm \
  -e FHIR_SERVER_URL=http://localhost:8099/openmrs/ws/fhir2/R4 \
  -e JDBC_MODE_ENABLED=true \
  -e PARQUET_PATH=/workspace/e2e-tests/JDBC_OPENMRS \
  -e OUTPUT_PARQUET_VIEW_PATH=/workspace/e2e-tests/JDBC_OPENMRS/VIEWS_TIMESTAMP_1 \
  -e SINK_PATH=http://localhost:9002/fhir \
  -e SINK_USERNAME=hapi -e SINK_PASSWORD=hapi \
  -e FHIR_DATABASE_CONFIG_PATH=/workspace/utils/dbz_event_to_fhir_config.json \
  -v $(pwd):/workspace \
  --network host \
  ${_REPOSITORY}/batch-pipeline:${_TAG}
```

### 7. Run e2e test for JDBC mode with OpenMRS source

```bash
./e2e-tests/pipeline_validation.sh \
  e2e-tests/ \
  JDBC_OPENMRS \
  JDBC_OPENMRS_FHIR_JSON \
  http://localhost:9002 \
  --openmrs
```

### 8. Test indicators

**Note:** This step can be skipped when running locally. It validates the
indicator calculation framework against a fixed set of pre-existing test
Parquet files (`test_files/parquet_big_db_r4`) — it does not use the OpenMRS
data processed above. It also runs BigQuery compatibility tests that require
Google Application Default Credentials, which are not available in a local
environment, causing the script to exit early due to `set -e`.

If you do want to run it (e.g., if you have GCP credentials configured), run:

```bash
cd dwh
./validate_indicators.sh
cd ..
```

### 9. Turn down OpenMRS and sink servers

```bash
docker compose -f docker/openmrs-compose.yaml -f docker/sink-compose.yml -p openmrs-project down
```

## Final Clean Up

After running the desired sections, stop all remaining stacks to free ports and
memory. Run only the commands relevant to the sections you executed:

```bash
# HAPI source server and its Postgres database
docker compose -f docker/hapi-compose.yml -p hapi-compose down -v

# Sink servers from the Initial Setup section
docker compose -f docker/sink-compose.yml -p sink-server-search down -v
docker compose -f docker/sink-compose.yml -p sink-server-jdbc down -v

# Sink server from the FHIR to FHIR Sync section
docker compose -f docker/sink-compose.yml -p sink-server down -v

# Controller and Spark stack
docker compose -f docker/compose-controller-spark-sql-single.yaml down -v

# OpenMRS server and its sink
docker compose -f docker/openmrs-compose.yaml -f docker/sink-compose.yml -p openmrs-project down -v
```
