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


################################## WAIT FOR START ############################# 
# Script used in e2e-test that waits for OpenMRS and FHIR server to start.
#
# Example usage:
#   ./wait_for_start.sh    
#   The above example waits for the OpenMRS source server on localhost
#
#   ./wait_for_start.sh --use_docker_network
#   The above example waits for the OpenMRS source server on docker network
#
#   ./wait_for_start.sh --use_docker_network --hapi
#   The above example waits for the HAPI source server on docker network

set -e

#################################################
# Function that defines the endpoints
# Globals:
#   FHIR_SERVER_URL
#   SINK_SERVER
# Arguments:
#   Flag whether to use docker network. By default, host URL is used. 
#   Flag whether a HAPI server is used as the source. By default, an OpenMRS server is used. 
#################################################
function setup() {  
  FHIR_SERVER_URL='http://localhost:8099/openmrs/ws/fhir2/R4'
  SINK_SERVER='http://localhost:8098'

  if [[ $1 = "--hapi" ]] || [[ $2 = "--hapi" ]]; then
    FHIR_SERVER_URL='http://localhost:8091'

    if [[ $1 = "--use_docker_network" ]] || [[ $2 = "--use_docker_network" ]]; then
      FHIR_SERVER_URL='http://hapi-server:8080'
      SINK_SERVER='http://sink-server:8080'
    fi

    hapi_server_check $FHIR_SERVER_URL
  else
    if [[ $1 = "--use_docker_network" ]] || [[ $2 = "--use_docker_network" ]]; then
      FHIR_SERVER_URL='http://openmrs:8080/openmrs/ws/fhir2/R4'
      SINK_SERVER='http://sink-server:8080'
    fi

    openmrs_server_check $FHIR_SERVER_URL
  fi

  hapi_server_check $SINK_SERVER
}

#################################################
# Function to check if fhir server completed initialization 
#################################################
function openmrs_server_check() {
  openmrs_start_wait_time=0
  contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 \
      --connect-timeout 5 --max-time 20 ${1}/Patient \
      2>/dev/null | cut -d ";" -f 1)
  until [[ ${contenttype} == "application/fhir+json" ]]; do
    echo "WAITING FOR OPENMRS SERVER TO START"
    sleep 60s
    contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 \
      --connect-timeout 5 --max-time 20 ${1}/Patient \
      2>/dev/null | cut -d ";" -f 1)
    ((openmrs_start_wait_time += 1))
    if [[ ${openmrs_start_wait_time} == 20 ]]; then
      echo "TERMINATING TEST AS OPENMRS TOOK TOO LONG TO START"
      exit 1
    fi
  done
  echo "OPENMRS SERVER STARTED SUCCESSFULLY"
}

#################################################
# Function to check if HAPI server completed initialization 
#################################################
function hapi_server_check() {
  fhir_server_start_wait_time=0
  fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
  -u hapi:hapi --connect-timeout 5 --max-time 20 \
  ${1}/fhir/Observation 2>/dev/null)
  until [[ ${fhir_server_status_code} -eq 200 ]]; do
    sleep 1s
    echo "WAITING FOR FHIR SERVER TO START"
    fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
      -u hapi:hapi --connect-timeout 5 --max-time 20 \
      ${1}/fhir/Observation 2>/dev/null)
    ((fhir_server_start_wait_time += 1))
    if [[ fhir_server_start_wait_time == 10 ]]; then
      echo "TERMINATING AS FHIR SERVER TOOK TOO LONG TO START"
      exit 1
    fi
  done
  echo "FHIR SERVER STARTED SUCCESSFULLY"
}

setup $1 $2
