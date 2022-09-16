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

FROM adoptopenjdk/maven-openjdk11

ARG WORK_DIR="/usr/src/Main"
COPY target/streaming-binlog-bundled-0.1.0-SNAPSHOT.jar ${WORK_DIR}/app.jar
WORKDIR ${WORK_DIR}
ENV FHIR_SERVER_URL="http://openmrs:8080/openmrs/ws/fhir2/R4"
ENV FHIR_SERVER_USERNAME="admin"
ENV FHIR_SERVER_PASSWORD="Admin123"
ENV SINK_URL=""
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""
ENV PARQUET_PATH="/tmp/"
ENV FHIR_DEBEZIUM_CONFIG_PATH="/workspace/utils/dbz_event_to_fhir_config.json"
ENV JDBC_FETCH_SIZE=10000
ENV JDBC_MAX_POOL_SIZE=50
ENV JDBC_INITIAL_POOL_SIZE=10
ENV JDBC_DRIVER_CLASS="com.mysql.cj.jdbc.Driver"
ENV SECONDS_TO_FLUSH_PARQUET_FILES=3600

ENTRYPOINT java -jar /usr/src/Main/app.jar \
           --fhirServerUrl=${FHIR_SERVER_URL} \
           --fhirServerUserName=${FHIR_SERVER_USERNAME}  \
           --fhirServerPassword=${FHIR_SERVER_PASSWORD} \
           --fhirSinkPath=${SINK_URL}  \
           --sinkUserName=${SINK_USERNAME} \
           --sinkPassword=${SINK_PASSWORD} \
           --outputParquetPath=${PARQUET_PATH} \
           --fhirDebeziumConfigPath=${FHIR_DEBEZIUM_CONFIG_PATH} \
           --jdbcDriverClass=${JDBC_DRIVER_CLASS} \
           --jdbcMaxPoolSize=${JDBC_MAX_POOL_SIZE} \
           --jdbcInitialPoolSize=${JDBC_INITIAL_POOL_SIZE} \
           --secondsToFlushParquetFiles=${SECONDS_TO_FLUSH_PARQUET_FILES}
