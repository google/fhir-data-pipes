#!/usr/bin/env bash

# Copyright 2022 Google LLC
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


################################## WAIT FOR HAPI START ############################# Script used in e2e-test that waits for HAPI server to start.

set -e

#################################################
# Function that defines the endpoints
# Globals:
#   HAPI_SERVER_URL
# Arguments:
#   Flag whether to use docker network. By default, host URL is  used. 
#################################################
function setup() {  
  HAPI_SERVER_URL='http://localhost:8098'

  if [[ $1 = "--use_docker_network" ]]; then
    FHIR_SERVER_URL='http://hapi-server:8080'
  fi
}

#################################################
# Function to check if HAPI server completed initialization 
# Globals:
#   HAPI_SERVER_URL
#################################################
function hapi_server_check() {
  fhir_server_start_wait_time=0
  fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
  -u hapi:hapi --connect-timeout 5 --max-time 20 \
  ${HAPI_SERVER_URL}/fhir/Observation 2>/dev/null)
  until [[ ${fhir_server_status_code} -eq 200 ]]; do
    sleep 1s
    echo "WAITING FOR HAPI SERVER TO START"
    fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
      -u hapi:hapi --connect-timeout 5 --max-time 20 \
      ${HAPI_SERVER_URL}/fhir/Observation 2>/dev/null)
    ((fhir_server_start_wait_time += 1))
    if [[ fhir_server_start_wait_time == 10 ]]; then
      echo "TERMINATING AS HAPI SERVER TOOK TOO LONG TO START"
      exit 1
    fi
  done
  echo "HAPI SERVER STARTED SUCCESSFULLY"
}

setup $1
hapi_server_check