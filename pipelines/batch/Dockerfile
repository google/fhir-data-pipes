# Copyright 2021-2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# If you are running locally, overwrite any of the ENV vars you see here in the 
# run-batch-local.sh script, before running the script. The script will build
# and run the docker image for you.  


FROM eclipse-temurin:17-jdk-focal

# TODO: Investigate why switching to the next base image, breaks the e2e in a
# strange way, i.e., in one of the last steps, when running JDBC version of the
# pipeline against OpenMRS (with a DB connection error)!
# FROM eclipse-temurin:11-jdk-focal

ARG WORK_DIR="/usr/src/Main"
COPY target/batch-bundled.jar ${WORK_DIR}/app.jar
WORKDIR ${WORK_DIR}

ENV FHIR_SERVER_URL="http://openmrs:8080/openmrs/ws/fhir2/R4"
ENV FHIR_SERVER_USERNAME="admin"
ENV FHIR_SERVER_PASSWORD="Admin123"
ENV SINK_PATH=""
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""
ENV RESOURCE_LIST="Patient,Encounter,Observation"
ENV BATCH_SIZE=100
ENV TARGET_PARALLELISM=10
ENV PARQUET_PATH="/tmp/"
ENV JDBC_MODE_ENABLED=false
ENV JDBC_FETCH_SIZE=10000
ENV JDBC_MAX_POOL_SIZE=50
ENV JDBC_INITIAL_POOL_SIZE=10
ENV NUM_FILE_SHARDS=3
ENV FHIR_DATABASE_CONFIG_PATH="/workspace/utils/hapi-postgres-config.json"
ENV JDBC_MODE_HAPI=false
ENV RUNNER="DirectRunner"
ENV FHIR_FETCH_MODE="FHIR_SEARCH"
ENV VIEW_DEFINITIONS_DIR="/workspace/docker/config/views/"
# This is to simulate a real controller scenario (this is assumed in e2e-tests).
ENV OUTPUT_PARQUET_VIEW_PATH="/tmp/VIEWS_TIMESTAMP_1/"


ENTRYPOINT java -jar /usr/src/Main/app.jar  \
           --fhirServerUrl=${FHIR_SERVER_URL} --fhirServerUserName=${FHIR_SERVER_USERNAME} --fhirServerPassword=${FHIR_SERVER_PASSWORD} \
           --resourceList=${RESOURCE_LIST} --batchSize=${BATCH_SIZE} --targetParallelism=${TARGET_PARALLELISM} \
           --fhirSinkPath=${SINK_PATH}  --sinkUserName=${SINK_USERNAME} --sinkPassword=${SINK_PASSWORD} \
           --outputParquetPath=${PARQUET_PATH} --jdbcModeEnabled=${JDBC_MODE_ENABLED} \
           --jdbcMaxPoolSize=${JDBC_MAX_POOL_SIZE} \
           --fhirDatabaseConfigPath=${FHIR_DATABASE_CONFIG_PATH} --jdbcInitialPoolSize=${JDBC_INITIAL_POOL_SIZE} \
           --jdbcFetchSize=${JDBC_FETCH_SIZE} --jdbcModeHapi=${JDBC_MODE_HAPI} --runner=${RUNNER} \
           --fhirFetchMode=${FHIR_FETCH_MODE} \
           --viewDefinitionsDir=${VIEW_DEFINITIONS_DIR} \
           --outputParquetViewPath=${OUTPUT_PARQUET_VIEW_PATH}
