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

## Step-by-step local reproduction

### 1. Start shared services

Start the HAPI source and both sink servers the same way Cloud Build does:

```bash
docker compose -f docker/hapi-compose.yml -p hapi-compose up --force-recreate --remove-orphans -d

SINK_SERVER_NAME=sink-server-search SINK_SERVER_PORT=9001 docker compose -f docker/sink-compose.yml -p sink-server-search up --force-recreate --remove-orphans -d

SINK_SERVER_NAME=sink-server-jdbc SINK_SERVER_PORT=9002 docker compose -f docker/sink-compose.yml -p sink-server-jdbc up --force-recreate --remove-orphans -d
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
$ . ./venv/bin/activate
```

or if you use the python standard library `venv` module, you can do it by
running:

```bash
$ python3 -m venv venv
$ source ./venv/bin/activate
```

Then, you can then install the requirements with:

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

### 4. Build the e2e controller/spark image

```bash
docker build -t ${_REPOSITORY}/e2e-tests/controller-spark:${_TAG} -f e2e-tests/controller-spark/Dockerfile .
```

### 5. Build and run the batch pipeline

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

**Note:** The /workspace/e2e-tests/FHIR_SEARCH_HAPI/VIEWS_TIMESTAMP_1 folder is
hardcoded as the output path for the parquet views and subsequent runs may
overwrite the content. Make sure to clean it up between runs if needed.

Repeat equivalent runs for JDBC mode, Bulk Export mode, and OpenMRS flows by
applying the same env vars/cloud build steps from `cloudbuild.yaml`.

### 6. Run e2e tests

**Note:** Only run this step if the E2E tests are failing; otherwise skip.

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

Repeat for each mode (JDBC, Bulk Export e.t.c) using the matching environment
variables from the original `cloudbuild.yaml`.

### 7. Clean up containers

When finished, bring everything down to avoid port conflicts:

```bash
docker compose -f docker/sink-compose.yml -p sink-server-search down -v
docker compose -f docker/sink-compose.yml -p sink-server-jdbc down -v
docker compose -f docker/hapi-compose.yml -p hapi-compose down -v
```

Also stop the controller/spark Compose services and other environment-specific
stacks as needed.

### 8. Additional steps

Inspect `cloudbuild.yaml` to discover any additional `run` steps you wish to
reproduce locally (e.g., `dwh/validate_indicators.sh`, controller tests, OpenMRS
flows).

## Conclusion

By following these steps, you can reproduce the main parts of `cloudbuild.yaml`
locally and iterate on fixes without waiting for Cloud Build. Adjust the script
sequence to match your debugging focus, and optionally wrap common sections in
reusable shell scripts.
