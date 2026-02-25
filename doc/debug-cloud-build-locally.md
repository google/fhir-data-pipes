# Running `cloudbuild.yaml` Locally with Docker

`cloud-build-local` is deprecated; however you can still iterate on the
`cloudbuild.yaml` steps by running them manually using Docker and the same
tooling that Cloud Build uses. This document provides a lightweight, repeatable
workflow for running the CI pipeline locally while being able to iterate
quickly.

**Note:** Depending on what stage of the build is failing for your PR, you can
bring up only services that are relevant to that stage.

## Prerequisites

- Docker installed (including `docker compose`); one way of achieving this is to
  install Docker Desktop.
- A local JDK 17 + Maven 3.8.x for the Maven build steps.
- The GitHub repo is checked out locally.

## Environment setup

1. Create an external Docker network named `cloudbuild`:

```bash
docker network create cloudbuild
```

2. From the repo root, export substitutions that Cloud Build normally injects:

```bash
export _TAG=local
export _REPOSITORY=fhir-analytics
```

## Initial Setup

### 1. Start shared services

Start the HAPI source and both sink servers the same way Cloud Build does:

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

Wait for the servers to become ready and confirm via the command:

```bash
docker ps
```

You should get output similar to:

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

Upload sample synthetic data to HAPI stored in
[sample_data](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/sample_data)
to the FHIR server that you brought up using the
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

Run the uploader script to upload the synthetic data to the HAPI FHIR server
brought up in the previous step with:

```bash
python3 ./synthea-hiv/uploader/main.py HAPI http://localhost:9001/fhir \
--input_dir ./synthea-hiv/sample_data --cores 8
```

Depending on your machine, using too many cores may slow down your machine or
cause JDBC connection pool errors with the HAPI FHIR server. Reducing the number
of cores using the `--cores` flag should help at the cost of increasing the time
to upload the data.

### 4. Build and run the batch pipeline

**Note:** this step is only needed if the E2E is failing in the batch pipeline
step; otherwise skip.

Run this command to build Pipeline images (or reuse a previously built image)
with:

```bash
cd pipelines/batch
docker build -t ${_REPOSITORY}/batch-pipeline:${_TAG} .
cd ../..
```

Run the FHIR-search job against HAPI:

```bash
docker run --rm \
  -e FHIR_SERVER_URL=http://localhost:9001/fhir \
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

Execute the HAPI search test. On the cloud build, this is done via a Docker run:

```bash
  docker run --rm -e PARQUET_SUBDIR=FHIR_SEARCH_HAPI   \
    -e FHIR_JSON_SUBDIR=FHIR_SEARCH_HAPI_JSON  \
    -e SINK_SERVER=http://localhost:9001 \
    -e DOCKER_NETWORK=host   \
    -v $(pwd):/workspace   \
    --network host    \
    ${_REPOSITORY}/e2e-tests:${_TAG}
```

locally, you can run the equivalent script directly:

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ FHIR_SEARCH_HAPI FHIR_SEARCH_HAPI_JSON http://localhost:9001/fhir
```

## JDBC Mode

**Note:** Only run this section if debugging JDBC mode; otherwise skip.

### 1. Run batch pipeline for JDBC mode

Run the JDBC mode job against HAPI:

```bash
docker run --rm \
  -e JDBC_MODE_ENABLED=true \
  -e JDBC_MODE_HAPI=true \
  -e FHIR_SERVER_URL=http://localhost:9001/fhir \
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
./e2e-tests/pipeline_validation.sh e2e-tests/ JDBC_HAPI JDBC_HAPI_FHIR_JSON http://localhost:9002/fhir
```

## Bulk Export Mode

**Note:** Only run this section if debugging Bulk Export mode; otherwise skip.

### 1. Run batch pipeline for Bulk Export mode

```bash
docker run --rm \
  -e FHIR_FETCH_MODE=BULK_EXPORT \
  -e FHIR_SERVER_URL=http://localhost:9001/fhir \
  -e PARQUET_PATH=/workspace/e2e-tests/BULK_EXPORT \
  -e OUTPUT_PARQUET_VIEW_PATH=/workspace/e2e-tests/BULK_EXPORT/VIEWS_TIMESTAMP_1 \
  -v $(pwd):/workspace \
  --network host \
  ${_REPOSITORY}/batch-pipeline:${_TAG}
```

### 2. Run e2e tests for Bulk Export mode

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ BULK_EXPORT BULK_EXPORT_FHIR_JSON
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

```bash
docker run --rm --network host postgres psql -U admin -d postgres -h localhost -p 5432 -c 'CREATE DATABASE views;'
```

### 3. Build the controller/spark image

```bash
docker build -t ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG} -f e2e-tests/controller-spark/Dockerfile .
```

### 4. Bring up controller and Spark containers

```bash
PIPELINE_CONFIG=/workspace/docker/config \
DWH_ROOT=/workspace/e2e-tests/controller-spark/dwh \
docker compose \
  -f docker/compose-controller-spark-sql-single.yaml \
  up --force-recreate -d
```

### 5. Run e2e test for controller and Spark

```bash
docker run --rm -v $(pwd):/workspace ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG}
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
SINK_SERVER_NAME=sink-server-controller \
SINK_SERVER_PORT=9001 \
docker compose \
  -f docker/sink-compose.yml \
  -p sink-server \
  up --force-recreate -d
```

### 2. Bring up pipeline controller for FHIR to FHIR sync

**Note:** You can use the controller from the previous step if you have it
running

```bash
PIPELINE_CONFIG=/workspace/docker/config \
DWH_ROOT=/workspace/e2e-tests/controller-spark/dwh \
FHIRDATA_SINKFHIRSERVERURL=http://localhost:9001/fhir \
FHIRDATA_GENERATEPARQUETFILES=false \
FHIRDATA_CREATEHIVERESOURCETABLES=false \
FHIRDATA_CREATEPARQUETVIEWS=false \
FHIRDATA_SINKDBCONFIGPATH= \
docker compose \
  -f docker/compose-controller-spark-sql-single.yaml \
  up --force-recreate --no-deps -d pipeline-controller
```

### 3. Run e2e test for FHIR to FHIR sync

```bash
docker run --rm -e DWH_TYPE=FHIR -v $(pwd):/workspace ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG}
```

### 4. Bring down pipeline controller and sink

```bash
docker compose -f docker/compose-controller-spark-sql-single.yaml down -v
docker compose -f docker/sink-compose.yml -p sink-server down -v
```

## OpenMRS Mode

**Note:** Only run this section if debugging OpenMRS mode; otherwise skip.

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

### 2. Upload to OpenMRS

```bash
python3 ./synthea-hiv/uploader/main.py OpenMRS \
http://localhost:8080/openmrs/ws/fhir2/R4 --convert_to_openmrs \
--input_dir ./synthea-hiv/sample_data --input_dir ./synthea-hiv/sample_data
```

### 3. Run batch pipeline for FHIR-search mode with OpenMRS source

```bash
docker run --rm \
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
  http://localhost:9002/fhir --openmrs
```

### 5. Run batch pipeline for JDBC mode with OpenMRS source

```bash
docker run --rm \
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

### 6. Run e2e test for JDBC mode with OpenMRS source

```bash
./e2e-tests/pipeline_validation.sh \
  e2e-tests/ \
  JDBC_OPENMRS \
  JDBC_OPENMRS_FHIR_JSON \
  http://localhost:9002/fhir \
  --openmrs
```

### 7. Test indicators

```bash
cd dwh
./validate_indicators.sh
cd ..
```

### 8. Turn down OpenMRS and sink servers

```bash
docker compose -f docker/openmrs-compose.yaml -f docker/sink-compose.yml -p openmrs-project down
```

### 9. Turn down HAPI source server

```bash
docker compose -f docker/hapi-compose.yml down
```

## Final Clean Up

### 1. Clean up remaining containers

After running the desired sections, ensure all containers are stopped to avoid
port conflicts.

Stop any remaining controller/spark Compose services and other
environment-specific stacks as needed.
