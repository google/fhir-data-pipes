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

ENV SINK_PATH="http://localhost:8098/fhir"
ENV SINK_USERNAME=""
ENV SINK_PASSWORD=""
ENV SERVER_URL="http://localhost:8091/openmrs"
ENV SEARCH_LIST="Patient"
ENV BATCH_SIZE=10
ENV TARGET_PARALLELISM=10


RUN echo 'Batch Job started'
VOLUME [ "${WORK_DIR}" ]

ENTRYPOINT java -cp app.jar org.openmrs.analytics.FhirEtl --serverUrl=${SERVER_URL} \
           --searchList=${SEARCH_LIST} --batchSize=${BATCH_SIZE} --targetParallelism=${TARGET_PARALLELISM}  \
           --sinkPath=${SINK_PATH}  --sinkUsername=${SINK_USERNAME} --sinkPassword=${SINK_PASSWORD}