# Running `cloudbuild.yaml` Locally with Docker

`cloudbuild-local` is deprecated; however you can still iterate on the
`cloudbuild.yaml` steps by running them manually using Docker and the same
tooling that Cloud Build uses. This document provides a lightweight, repeatable
workflow for running the CI pipeline locally while being able to iterate
quickly.

## Prerequisites

- Docker Desktop (with `docker compose`) installed and running.
- A local JDK 17 + Maven 3.8.x for the Maven build steps.
- `python3` (for `synthea-uploader` unit tests) and `python -m unittest`
  available on `$PATH`.
- Access to the workspace root (same directory that holds `cloudbuild.yaml`).

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

3. If you need to mirror the GCP `gcloud` network configuration (Docker Compose
   services talk to each other by name), run all commands inside this directory
   so the Compose files resolve relative paths correctly.

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

### 3. Upload sample synthetic data to HAPI stored in

[sample_data](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/sample_data)
to the FHIR server that you brought up using the
[Synthea data uploader](https://github.com/google/fhir-data-pipes/blob/master/synthea-hiv/README.md#Uploader).

The uploader requires that you install the `uploader` module requirements first.
You can do this by running:

```bash
pip3 install -r ./synthea-hiv/uploader/requirements.txt
```

Please note, it is a good idea to first create a Python `virtualenv` before
running the above command to avoid conflicts with other Python packages you may
have installed globally. You can do this by running:

```bash
$ virtualenv -p python3.8 venv
$ . ./venv/bin/activate
```

If you had this already set up, just activate your existing virtualenv.

```bash
$ workon venv
```

Then, you can run the uploader script to upload the synthetic data to the FHIR
server.

For example, to upload to the HAPI FHIR server brought up in the previous step,
run:

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

Use the Maven-built image (or rebuild with `pipelines/batch/docker` if you
change it):

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

Repeat equivalent runs for JDBC mode, Bulk Export mode, and OpenMRS flows by
applying the same env vars/cloud build steps from `cloudbuild.yaml`.

### 6. Run e2e tests

Execute the HAPI search test:

```bash
  docker run --rm -e PARQUET_SUBDIR=FHIR_SEARCH_HAPI   \
    -e FHIR_JSON_SUBDIR=FHIR_SEARCH_HAPI_JSON  \
    -e SINK_SERVER=http://localhost:9001 \
    -e DOCKER_NETWORK=host   \
    -v $(pwd):/workspace   \
    --network host    \
    ${_REPOSITORY}/e2e-tests:${_TAG}
```

or just the script directly:

```bash
./e2e-tests/pipeline_validation.sh e2e-tests/ FHIR_SEARCH_HAPI FHIR_SEARCH_HAPI_JSON http://localhost:9001/fhir
```

Repeat for each mode (JDBC, Bulk Export, controller/spark, OpenMRS) using the
matching environment variables from the original `cloudbuild.yaml`.

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
