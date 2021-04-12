# Copyright 2020 Google LLC
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

FROM openjdk:8

ARG WORK_DIR="/usr/src/Main"
COPY target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar ${WORK_DIR}/app.jar
WORKDIR ${WORK_DIR}

ENV OPENMRS_URL="http://localhost:8099/openmrs"
ENV OPENMRS_USERNAME="admin"
ENV OPENMRS_PASSWORD="Admin123"
ENV SINK_PATH=""
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""
ENV RESOURCE_LIST="Patient,Encounter,Observation"
ENV BATCH_SIZE=10
ENV TARGET_PARALLELISM=10
ENV PARQUET_PATH="/tmp/"
ENV JDBC_MODE_ENABLED=false
ENV JDBC_DRIVER_CLASS="com.mysql.cj.jdbc.Driver"
ENV DB_USER="root"
ENV DB_PASSWORD="debezium"
ENV JDBC_FETCH_SIZE=10000
ENV JDBC_MAX_POOL_SIZE=50
ENV JDBC_INITIAL_POOL_SIZE=10
ENV JDBC_URL="jdbc:mysql://localhost:3306/openmrs"
ENV NUM_FILE_SHARDS=3


RUN echo 'Batch Job started'
VOLUME [ "${WORK_DIR}" ]

ENTRYPOINT java -cp app.jar org.openmrs.analytics.FhirEtl \
           --openmrsServerUrl=${OPENMRS_URL} --openmrsUserName=${OPENMRS_USERNAME} --openmrsPassword=${OPENMRS_PASSWORD} \
           --resourceList=${RESOURCE_LIST} --batchSize=${BATCH_SIZE} --targetParallelism=${TARGET_PARALLELISM} \
           --fhirSinkPath=${SINK_PATH}  --sinkUserName=${SINK_USERNAME} --sinkPassword=${SINK_PASSWORD} \
           --outputParquetPath=${PARQUET_PATH} --jdbcModeEnabled=${JDBC_MODE_ENABLED} --jdbcDriverClass=${JDBC_DRIVER_CLASS} \
           --dbUser=${DB_USER} --dbPassword=${DB_PASSWORD} --jdbcMaxPoolSize=${JDBC_MAX_POOL_SIZE} --jdbcUrl=${JDBC_URL} \
           --jdbcInitialPoolSize=${JDBC_INITIAL_POOL_SIZE} --jdbcFetchSize=${JDBC_FETCH_SIZE} --numFileShards=${NUM_FILE_SHARDS}
