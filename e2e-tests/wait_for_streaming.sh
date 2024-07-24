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
#
# Example usage:
#   ./wait_for_streaming.sh --FHIR_SERVER_URL=http://openmrs:8080 --SINK_SERVER=http://sink-server-for-openmrs:8080

set -e

#################################################
# Set the global variables
# Globals:
#   FHIR_SERVER_URL
#   SINK_SERVER
#################################################
while [ $# -gt 0 ]; do
  case "$1" in
    --FHIR_SERVER_URL=*)
      FHIR_SERVER_URL="${1#*=}"
      ;;
    --SINK_SERVER=*)
      SINK_SERVER="${1#*=}"
      ;;
    *)
      printf "Error: Invalid argument %s" "$1"
      exit 1
  esac
  shift
done

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
    -u ${FHIR_SERVER_URL}/openmrs/ws/fhir2/R4  -o /workspace/e2e-tests/STREAMING \
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
    ${FHIR_SERVER_URL}/openmrs/ws/fhir2/R4 \
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

start_pipeline
wait_for_sink