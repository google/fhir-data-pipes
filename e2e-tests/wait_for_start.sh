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
# Script used in e2e-test that waits for OpenMRS and FHIR servers to start.
#
# Example usage:
#   ./wait_for_start.sh --HAPI_SERVER_URLS=http://hapi-server1:8080,http://hapi-server2:8080 --OPENMRS_SERVER_URLS=http://openmrs:8080
#   The above example waits for two hapi servers and one openmrs server to start

#################################################
# Set the global variables
# Globals:
#   HAPI_SERVER_URLS
#   OPENMRS_SERVER_URLS
#################################################
while [ $# -gt 0 ]; do
  case "$1" in
    --HAPI_SERVER_URLS=*)
      HAPI_SERVER_URLS="${1#*=}"
      ;;
    --OPENMRS_SERVER_URLS=*)
      OPENMRS_SERVER_URLS="${1#*=}"
      ;;
    *)
      printf "Error: Invalid argument %s" "$1"
      exit 1
  esac
  shift
done

#################################################
# Function that waits for all the Hapi and OpenMRS servers to start
#################################################
function wait_for_servers_to_start() {
  if [ -n "$HAPI_SERVER_URLS" ]; then
    IFS=',' read -r -a array <<< "$HAPI_SERVER_URLS"
    for url in "${array[@]}"
    do
      hapi_server_check "$url"
    done
  fi

  if [ -n "$OPENMRS_SERVER_URLS" ]; then
    IFS=',' read -r -a array <<< "$OPENMRS_SERVER_URLS"
    for url in "${array[@]}"
    do
     openmrs_server_check "$url/openmrs/ws/fhir2/R4"
    done
  fi
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
  echo "OPENMRS SERVER ${1} STARTED SUCCESSFULLY"
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
    sleep 30s
    echo "WAITING FOR FHIR SERVER TO START"
    fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
      -u hapi:hapi --connect-timeout 5 --max-time 20 \
      ${1}/fhir/Observation 2>/dev/null)
    ((fhir_server_start_wait_time += 1))
    if [[ $fhir_server_start_wait_time == 20 ]]; then
      echo "TERMINATING AS FHIR SERVER TOOK TOO LONG TO START"
      exit 1
    fi
  done
  echo "FHIR SERVER ${1} STARTED SUCCESSFULLY"
}

wait_for_servers_to_start
