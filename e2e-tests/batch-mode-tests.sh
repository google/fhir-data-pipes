#!/usr/bin/env bash
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

#######################################
# Function that prints messages.
# Arguments:
#   anything that needs printing
#######################################
function print_message() {
  local print_prefix="E2E: BATCH MODE TEST:"
  echo "${print_prefix} $*"
}

#######################################
# Call common.sh to setup 
# the testing env
#  
#######################################
source ./e2e-tests/common-mode-tests.sh
setup

#######################################
# Function that tests sinking to parquet files
# and compares output to what is in openmrs server
# Arguments:
#   extra flags for Dexex.args
#   file path
#   the mode to test: FHIR Search vs JDBC
#######################################
function test_parquet_sink() {
  local command=(mvn exec:java -pl batch "-Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs \
        --openmrsUserName=admin --openmrsPassword=Admin123 \
        --resourceList=Patient,Encounter,Observation --batchSize=20 $1")
  local test_dir=$2
  local mode=$3
  print_message "PARQUET FILES WILL BE WRITTEN INTO ${test_dir} DIRECTORY"
  print_message "RUNNING BATCH MODE WITH PARQUET SINK FOR $2 MODE"
  print_message " ${command[*]}"
  "${command[@]}"

  if [[ -z $(ls "${test_dir}"/Patient) ]]; then
    print_message "PARQUET FILES NOT CREATED"
    exit 1
  fi
  print_message "PARQUET FILES OUTPUT DIR IS ${test_dir}"
  print_message "COPYING PARQUET TOOLS JAR FILE TO ROOT"
  cp e2e-tests/parquet-tools-1.11.1.jar "${test_dir}"
  if ! [[ $(command -v jq) ]]; then
    print_message "COULD NOT INSTALL JQ, INSTALL MANUALLY TO CONTINUE RUNNING THE TEST"
    exit 1
  fi
  cd "${test_dir}"
  validate_parquet_counts
}

#######################################
# Function that tests sinking to FHIR server
# and compares output to what is in openmrs server
# Arguments:
#   extra flags for Dexex.args
#   file path
#   the mode to test: FHIR Search vs JDBC
#######################################
function test_fhir_sink() {
  local command=(mvn exec:java -pl batch "-Dexec.args=--resourceList=Patient --batchSize=20  \
  --fhirSinkPath=http://localhost:8098/fhir  --sinkUserName=hapi --sinkPassword=hapi $1")
  local test_dir=$2
  local mode=$3
  print_message " ${command[*]}"
  "${command[@]}"
  cd "${test_dir}"
  validate_fhir_counts 
}

setup
print_message "---- STARTING PARQUET SINK TEST ----"
test_parquet_sink "--outputParquetPath=${TEST_DIR_FHIR}/" "${TEST_DIR_FHIR}" "FHIR_SEARCH"
test_parquet_sink "--outputParquetPath=${TEST_DIR_JDBC}/ ${JDBC_SETTINGS}" "${TEST_DIR_JDBC}" "JDBC"

# TODO(omarismail) merge so that both FHIR Sink test and Parquet Sink test are one
print_message "---- STARTING FHIR SINK TEST ----"
test_fhir_sink "${JDBC_SETTINGS}" "${TEST_DIR_JDBC}" "JDBC"
test_fhir_sink "" "${TEST_DIR_FHIR}" "FHIR_SEARCH"
print_message "END!!"
