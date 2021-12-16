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

# BUILD STAGE
FROM adoptopenjdk/maven-openjdk8 as build
WORKDIR "/usr/src"

# Selective Adds to Speed Up Build
ADD ../pipelines/common ./pipelines/common
ADD ../pipelines/streaming-binlog/src ./pipelines/streaming-binlog/src 
ADD ../pipelines/streaming-binlog/pom.xml ./pipelines/streaming-binlog/pom.xml 
ADD ../pipelines/batch/pom.xml ./pipelines/batch/pom.xml
ADD ../pipelines/streaming-atomfeed/pom.xml ./pipelines/streaming-atomfeed/pom.xml
ADD ../pipelines/batch/pom.xml ./pipelines/batch/pom.xml
ADD ../pipelines/pom.xml ./pipelines/pom.xml
ADD ../bunsen ./bunsen
ADD ../pom.xml ./

RUN mvn clean package -DskipTests -Dlicense.skip=true -pl pipelines/streaming-binlog -am -B -q

RUN /bin/bash -l -c 'echo echo "$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout=true)" > /etc/profile.d/pom_version.sh'

RUN chmod u+x /etc/profile.d/pom_version.sh

RUN POM_VERSION=`/etc/profile.d/pom_version.sh` && cp /usr/src/pipelines/streaming-binlog/target/streaming-binlog-bundled-$POM_VERSION.jar /usr/src/app.jar

# RUN STAGE
FROM adoptopenjdk/maven-openjdk8 as run

LABEL org.opencontainers.image.source https://github.com/i-tech-uw/openmrs-fhir-analytics

WORKDIR /usr/src

# Copy over JAR file
COPY --from=build /usr/src/app.jar app.jar

# Default ENV settings
ENV OPENMRS_URL="http://openmrs:8080/openmrs"
ENV OPENMRS_USERNAME="admin"
ENV OPENMRS_PASSWORD="Admin123"
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

ENTRYPOINT java -jar /usr/src/app.jar \
           --openmrsServerUrl=${OPENMRS_URL}  \
           --openmrsUserName=${OPENMRS_USERNAME}  \
           --openmrsPassword=${OPENMRS_PASSWORD} \
           --fhirSinkPath=${SINK_URL}  \
           --sinkUserName=${SINK_USERNAME} \
           --sinkPassword=${SINK_PASSWORD} \
           --outputParquetPath=${PARQUET_PATH} \
           --fhirDebeziumConfigPath=${FHIR_DEBEZIUM_CONFIG_PATH} \
           --jdbcDriverClass=${JDBC_DRIVER_CLASS} \
           --jdbcMaxPoolSize=${JDBC_MAX_POOL_SIZE} \
           --jdbcInitialPoolSize=${JDBC_INITIAL_POOL_SIZE} \
           --secondsToFlushParquetFiles=${SECONDS_TO_FLUSH_PARQUET_FILES}
