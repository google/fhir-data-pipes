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

COPY target/fhir-binlog-streaming-etl-bundled-0.1.0-SNAPSHOT.jar /deployments/app.jar
WORKDIR /deployments
ENV OPENMRS_URL="http://localhost:8099/openmrs"
ENV OPENMRS_USERNAME="admin"
ENV OPENMRS_PASSWORD="Admin123"
ENV SINK_URL=""
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""
ENV PARQUET_PATH="/tmp/"
ENV FHIR_DEBEZIUM_CONFIG_PATH="/deployments/dbz_event_to_fhir_config.json"
ENV JDBC_FETCH_SIZE=10000
ENV JDBC_MAX_POOL_SIZE=50
ENV JDBC_INITIAL_POOL_SIZE=10
ENV JDBC_URL="jdbc:mysql://localhost:3306/openmrs"
ENV JDBC_DRIVER_CLASS="com.mysql.cj.jdbc.Driver"

RUN echo "Debezium Streaming Job started"

ENTRYPOINT java -cp app.jar org.openmrs.analytics.Runner \
           --openmrsServerUrl=${OPENMRS_URL}  \
           --openmrsUserName=${OPENMRS_USERNAME}  \
           --openmrsPassword=${OPENMRS_PASSWORD} \
           --fhirSinkPath=${SINK_URL}  \
           --sinkUserName=${SINK_USERNAME} \
           --sinkPassword=${SINK_PASSWORD} \
           --fileParquetPath=${PARQUET_PATH} \
           --fhirDebeziumConfigPath=${FHIR_DEBEZIUM_CONFIG_PATH} \
           --jdbcDriverClass=${JDBC_DRIVER_CLASS} \
           --jdbcMaxPoolSize=${JDBC_MAX_POOL_SIZE} \
           --jdbcUrl=${JDBC_URL} \
           --jdbcInitialPoolSize=${JDBC_INITIAL_POOL_SIZE}
