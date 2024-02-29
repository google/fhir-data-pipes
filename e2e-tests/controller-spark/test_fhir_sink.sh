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
#   ./test_fhir_sink.sh PARQUET_SUBDIR STREAMING HOME_PATH PARQUET_SUBDIR \
#  SINK_FHIR_SERVER_URL TOTAL_TEST_PATIENTS TOTAL_TEST_ENCOUNTERS TOTAL_TEST_OBS


set -e

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
# Prints the usage
#################################################
function usage() {
  echo "This script Function counts resources in FHIR server and compares output to what is "
  echo "in the source FHIR server"
  echo " "
  echo " usage: ./test_fhir_sink.sh PARQUET_SUBDIR STREAMING HOME_PATH PARQUET_SUBDIR "
  echo "  SINK_FHIR_SERVER_URL TOTAL_TEST_PATIENTS TOTAL_TEST_ENCOUNTERS TOTAL_TEST_OBS"
}

#################################################
# Makes sure args passed are correct
#################################################
function validate_args() {
  if [[ $# -ne 8 ]]; then
    echo "Invalid number of arguments. Expected 8 arguments."
    usage
    exit 1
  fi
}

#################################################
# Function that defines the global vars
#################################################
function setup() {
    runMode=$1
    STREAMING=$2
    HOME_PATH=$3
    PARQUET_SUBDIR=$4
    SINK_FHIR_SERVER_URL=$5
    TOTAL_TEST_PATIENTS=$6
    TOTAL_TEST_ENCOUNTERS=$7
    TOTAL_TEST_OBS=$8
}


function test_fhir_sink(){
  local patient_query_param="?_summary=count"
  local enc_obs_query_param="?_summary=count"
  if [[ -n ${STREAMING} ]]; then 
      patient_query_param="?given=Alberta625"
      enc_obs_query_param="?subject.given=Alberta625"
  fi

  print_message "Finding number of patients, encounters and obs in FHIR server"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
  "${SINK_FHIR_SERVER_URL}/fhir/Patient${patient_query_param}" 2>/dev/null \
  >>"${HOME_PATH}/${PARQUET_SUBDIR}/patients-sink.json"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
  "${SINK_FHIR_SERVER_URL}/fhir/Encounter${enc_obs_query_param}" 2>/dev/null \
  >>"${HOME_PATH}/${PARQUET_SUBDIR}/encounters-sink.json"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
  "${SINK_FHIR_SERVER_URL}/fhir/Observation${enc_obs_query_param}" 2>/dev/null\
  >>"${HOME_PATH}/${PARQUET_SUBDIR}/obs-sink.json"

  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  local total_patients_sinked_fhir=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/patients-sink.json")
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  local total_encounters_sinked_fhir=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/encounters-sink.json")
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  local total_obs_sinked_fhir=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/obs-sink.json")
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ "${total_patients_sinked_fhir}" == "${TOTAL_TEST_PATIENTS}" && "${total_encounters_sinked_fhir}" \
      == "${TOTAL_TEST_ENCOUNTERS}" && "${total_obs_sinked_fhir}" == "${TOTAL_TEST_OBS}" ]] \
  ; then
  print_message "FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${runMode} MODE"
  else
  print_message "FHIR SERVER SINK TEST FAILED USING ${runMode} MODE"
  exit 1
  fi
}

validate_args  "$@"
setup "$@"
test_fhir_sink
