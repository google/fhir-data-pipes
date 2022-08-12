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

# Example usage:
#   ./pipeline_validation.sh ./ JDBC_OPENMRS
#   ./pipeline_validation.sh ./ NON_JDBC --use_docker_network
#   ./pipeline_validation.sh ./ STREAMING --use_docker_network

set -e

#################################################
# Prints the usage
#################################################
function usage() {
  echo "This script validates if number of resources sunk in parquet files and" 
  echo "FHIR Server match what is stored in the OpenMRS server"
  echo 
  echo " usage: ./pipeline_validation.sh  HOME_DIR  PARQUET_SUBDIR  [OPTIONS] "
  echo "    HOME_DIR          Path where e2e-tests directory is. Directory MUST"
  echo "                      contain the parquet tools jar as well as subdirectory"
  echo "                      of parquet file output"
  echo "    PARQUET_SUBDIR    Subdirectory name under HOME_DIR containing"
  echo "                      parquet files  "
  echo 
  echo " Options:  "
  echo "     --use_docker_network     Flag to specify whether to use docker"  
  echo "                              or host network URLs"
  echo "     --streaming              Flag to specify whether we are testing a"
  echo "                              streaming pipeline"
  echo "     --hapi                   Flag to specify whether we are testing a"
  echo "                              batch pipeline executed with jdbc direct"
  echo "                              fetch mode with HAPI as the source server"
}

#################################################
# Makes sure args passed are correct
#################################################
function validate_args() {
  if [[ $# -lt 2 || $# -gt 5  ]]; then
    echo "Invalid number of args passed."
    usage
    exit 1
  fi

  echo "Checking if the Parquet-tools JAR exists..."
  if compgen -G "${1}/parquet-tools*.jar" > /dev/null; then
    echo "Parquet-tools JAR exists in ${1}"
  else
    echo "Parquet-tools JAR not found in ${1}"
    usage
    exit 1
  fi

  if [[ ! -d ${1}/${2} ]]; then
    echo "The directory ${1}/${2} does not exist."
    usage
    exit 1
  fi
}


#################################################
# Function that prints messages
# Arguments:
#   anything that needs printing
#################################################
function print_message() {
  local print_prefix="E2E TEST:"
  echo "${print_prefix} $*"
}

#################################################
# Function that defines the global vars
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   FHIR_SERVER_URL
#   SINK_SERVER
#   STREAMING
#   HAPI
# Arguments:
#   Path where e2e-tests directory is. Directory contains parquet tools jar as 
#      well as subdirectory of parquet file output
#   Subdirectory name under HOME_DIR containing parquet files. 
#      Example: NON_JDBC or JDBC_OPENMRS
#   Optional: Flag to specify whether to use docker or host network URLs.
#   Optional: Flag to specify streaming pipeline test.
#   Optional: Flag to specify whether Jdbc mode is executed with a HAPI source server.
#################################################
function setup() {
  HOME_PATH=$1
  PARQUET_SUBDIR=$2
  rm -rf "${HOME_PATH}/fhir"
  rm -rf "${HOME_PATH}/${PARQUET_SUBDIR}/*.json"
  find "${HOME_PATH}/${PARQUET_SUBDIR}" -size 0 -delete
  FHIR_SERVER_URL='http://localhost:8099/openmrs/ws/fhir2/R4'
  SINK_SERVER='http://localhost:8098'
  STREAMING=""
  HAPI=""

  # TODO: We should refactor this code to parse the arguments by going through
  # each one and checking which ones are turned on.
  if [[ $3 = "--hapi" ]] || [[ $4 = "--hapi" ]] || [[ $5 = "--hapi" ]]; then
    HAPI="on"
    FHIR_SERVER_URL='http://localhost:8091'
  fi

  if [[ $3 = "--use_docker_network" ]] || [[ $4 = "--use_docker_network" ]] || [[ $5 = "--use_docker_network" ]]; then
    if [[ -n ${HAPI} ]]; then
        FHIR_SERVER_URL='http://hapi-server:8080'
    else
        FHIR_SERVER_URL='http://openmrs:8080/openmrs/ws/fhir2/R4'
    fi
    SINK_SERVER='http://sink-server:8080'
  fi

  if [[ $3 = "--streaming" ]] || [[ $4 = "--streaming" ]] || [[ $5 = "--streaming" ]]; then
    STREAMING="on"
  fi
}

#################################################
# Function to count resources in fhir server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   FHIR_SERVER_URL
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#   STREAMING
#   HAPI
#################################################
function fhir_source_query() {
  local patient_query_param="?_summary=count"
  local enc_obs_query_param="?_summary=count"
  local fhir_username="admin"
  local fhir_password="Admin123"
  local fhir_url_extension=""

  if [[ -n ${STREAMING} ]]; then 
      patient_query_param="?given=Alberta625"
      enc_obs_query_param="?subject.given=Alberta625"
  fi
  
  if [[ -n ${HAPI} ]]; then
      fhir_username="hapi"
      fhir_password="hapi"
      fhir_url_extension="/fhir"
  fi

  curl -L -X GET -u $fhir_username:$fhir_password --connect-timeout 5 --max-time 20 \
  "${FHIR_SERVER_URL}${fhir_url_extension}/Patient${patient_query_param}" 2>/dev/null >>"${HOME_PATH}/${PARQUET_SUBDIR}/patients.json"
  TOTAL_TEST_PATIENTS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/patients.json")
  print_message "Total FHIR source test patients ---> ${TOTAL_TEST_PATIENTS}"

  curl -L -X GET -u $fhir_username:$fhir_password --connect-timeout 5 --max-time 20 \
    "${FHIR_SERVER_URL}${fhir_url_extension}/Encounter${enc_obs_query_param}" \
    2>/dev/null >>"${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json"
  TOTAL_TEST_ENCOUNTERS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json")
  print_message "Total FHIR source test encounters ---> ${TOTAL_TEST_ENCOUNTERS}"

  curl -L -X GET -u $fhir_username:$fhir_password --connect-timeout 5 --max-time 20 \
    "${FHIR_SERVER_URL}${fhir_url_extension}/Observation${enc_obs_query_param}" \
    2>/dev/null >>"${HOME_PATH}/${PARQUET_SUBDIR}/obs.json"
  TOTAL_TEST_OBS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/obs.json")
  print_message "Total FHIR source test obs ---> ${TOTAL_TEST_OBS}"
}


#################################################
# Function that counts resources in parquet files and compares output to what 
#  is in openmrs server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#   HAPI
#################################################
function test_parquet_sink() {
  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/Patient/" | awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"
  total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/Encounter/" | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"
  total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/Observation/" | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"

  if [[ "${total_patients_streamed}" == "${TOTAL_TEST_PATIENTS}" && "${total_encounters_streamed}" \
        == "${TOTAL_TEST_ENCOUNTERS}" && "${total_obs_streamed}" == "${TOTAL_TEST_OBS}" ]] \
    ; then
    print_message "PARQUET SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
  else
    print_message "PARQUET SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
    exit 1
  fi
}

#################################################
# Function that counts resources in  FHIR server and compares output to what is 
#  in openmrs server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   SINK_SERVER
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#   STREAMING
#   HAPI
#################################################
function test_fhir_sink() {
  local patient_query_param="?_summary=count"
  local enc_obs_query_param="?_summary=count"

  if [[ -n ${STREAMING} ]]; then 
      patient_query_param="?given=Alberta625"
      enc_obs_query_param="?subject.given=Alberta625"
  fi
  print_message "Finding number of patients, encounters and obs in FHIR server"

  mkdir "${HOME_PATH}/fhir"
  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    "${SINK_SERVER}/fhir/Patient${patient_query_param}" 2>/dev/null >>"${HOME_PATH}/fhir/patients.json"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    "${SINK_SERVER}/fhir/Encounter${enc_obs_query_param}" 2>/dev/null >>"${HOME_PATH}/fhir/encounters.json"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    "${SINK_SERVER}/fhir/Observation${enc_obs_query_param}" 2>/dev/null >>"${HOME_PATH}/fhir/obs.json"

  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  total_patients_sinked_fhir=$(jq '.total' "${HOME_PATH}/fhir/patients.json")
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  total_encounters_sinked_fhir=$(jq '.total' "${HOME_PATH}/fhir/encounters.json")
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  total_obs_sinked_fhir=$(jq '.total' "${HOME_PATH}/fhir/obs.json")
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ "${total_patients_sinked_fhir}" == "${TOTAL_TEST_PATIENTS}" && "${total_encounters_sinked_fhir}" \
        == "${TOTAL_TEST_ENCOUNTERS}" && "${total_obs_sinked_fhir}" == "${TOTAL_TEST_OBS}" ]] \
    ; then
    print_message "FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
  else
    print_message "FHIR SERVER SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
    exit 1
  fi
}

validate_args  "$@"
setup "$@"
print_message "---- STARTING ${PARQUET_SUBDIR} TEST ----"
fhir_source_query 
test_parquet_sink 
test_fhir_sink 
print_message "END!!"
