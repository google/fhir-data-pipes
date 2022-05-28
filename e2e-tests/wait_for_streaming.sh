#!/usr/bin/env bash

# Copyright 2021 Google LLC
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

################################ WAIT FOR STREAMING ########################### Script used in e2e-test that waits for Streaming pipeline to sink resources

set -e

#################################################
# Function that defines the endpoints
# Globals:
#   FHIR_SERVER_URL
#   SINK_SERVER
# Arguments:
#   Flag whether to use docker network. By default, host URL is  used. 
#################################################
function setup() {  
  FHIR_SERVER_URL='http://localhost:8099/openmrs/ws/fhir2/R4'
  SINK_SERVER='http://localhost:8098'

  if [[ $1 = "--use_docker_network" ]]; then
    FHIR_SERVER_URL='http://openmrs:8080/openmrs/ws/fhir2/R4'
    SINK_SERVER='http://sink-server:8080'
  fi
}

#################################################
# Function that starts streaming pipeline
# Globals:
#   FHIR_SERVER_URL
#   SINK_SERVER
#################################################
function start_pipeline() {
    echo "STARTING STREAMING PIPELINE"
    cd /workspace/pipelines
    ../utils/start_pipelines.sh -s -streamingLog /workspace/e2e-tests/log.log \
    -u ${FHIR_SERVER_URL}  -o /workspace/e2e-tests/STREAMING \
    -secondsToFlushStreaming 5 -fhirSinkPath ${SINK_SERVER}/fhir \
    -sinkUsername hapi -sinkPassword hapi
}

#################################################
# Function that waits for streaming pipeline to sink resources
# Globals:
#   FHIR_SERVER_URL
#   SINK_SERVER
#################################################
function wait_for_sink() {
    local count=0
    cd /workspace
    python3 synthea-hiv/uploader/main.py OpenMRS \
    ${FHIR_SERVER_URL} \
    --input_dir e2e-tests/streaming_test_patient --convert_to_openmrs
    until [[ ${count} -ne 0 ]]; do
      # TODO: There seems to be a race condition here: we wait for results
      # to be ready in the sink FHIR store but by that time we also expect
      # the parquet output to be flushed (which is not necessarily the case).
      sleep 30s
      count=$(curl -u hapi:hapi -s ${SINK_SERVER}/fhir/Patient?_summary=count \
          | grep 'total' | awk '{print $NF}')
      echo "WAITING FOR RESOURCES TO SINK"
    done
    echo "RESOURCES IN FHIR SERVER"

}

setup $1
start_pipeline
wait_for_sink