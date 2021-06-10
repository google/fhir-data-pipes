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
# Function that sets up testing env
#######################################
function setup() {
  HOME_PATH=$(pwd)

  print_message "BUILDING THE ANALYTICS PROJECT"
  mvn compile

  print_message "STARTING SERVERS"
  docker-compose -f docker/openmrs-compose.yaml up -d --remove-orphans
  sleep 2m;
  openmrs_start_wait_time=0
  contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 --connect-timeout 5 \
    --max-time 20 http://localhost:8099/openmrs/ws/fhir2/R4/Patient 2>/dev/null | cut -d ";" -f 1)
  until [[ ${contenttype} == "application/fhir+json" ]]; do
    sleep 60s
    print_message "WAITING FOR OPENMRS SERVER TO START"
    contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 --connect-timeout 5 \
      --max-time 20 http://localhost:8099/openmrs/ws/fhir2/R4/Patient 2>/dev/null | cut -d ";" -f 1)
    ((openmrs_start_wait_time += 1))
    if [[ ${openmrs_start_wait_time} == 20 ]]; then
      print_message "TERMINATING TEST AS OPENMRS TOOK TOO LONG TO START"
      exit 1
    fi
  done
  print_message "OPENMRS SERVER STARTED SUCCESSFULLY"

  docker-compose -f  docker/sink-compose.yml up -d
  fhir_server_start_wait_time=0
  fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi --connect-timeout 5 \
    --max-time 20 http://localhost:8098/fhir/Observation 2>/dev/null)
  until [[ ${fhir_server_status_code} -eq 200 ]]; do
    sleep 1s
    print_message "WAITING FOR FHIR SERVER TO START"
    fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi --connect-timeout 5 \
      --max-time 20 http://localhost:8098/fhir/Observation 2>/dev/null)
    ((fhir_server_start_wait_time += 1))
    if [[ fhir_server_start_wait_time == 10 ]]; then
      print_message "TERMINATING AS FHIR SERVER TOOK TOO LONG TO START"
      exit 1
    fi
  done
  print_message "FHIR SERVER STARTED SUCCESSFULLY"

  TEST_DIR_FHIR=$(mktemp -d -t analytics_tests__XXXXXX_FHIRSEARCH)
  TEST_DIR_JDBC=$(mktemp -d -t analytics_tests__XXXXXX_JDBC)
  JDBC_SETTINGS="--jdbcModeEnabled=true"
}

############################################################################################################
# Function that validates parquet counts for batch and stream mode
# Arguments:
#    $1 The 'Patient' query parameter, e.g., '?family=Clive' or empty to fetch all patients
#    $2 The 'Encounter' query parameter, e.g., '?subject.given=Cliff' or empty to fetch all encounters
#    $3 The 'Observation' query parameter, e.g., '?subject.given=Cliff' or empty to fetch all observations
###########################################################################################################
function validate_parquet_counts() {
  mkdir omrs
  local STREAM_SUCCESSFUL="E2E: STREAM MODE TEST: STREAMING MODE WITH PARQUET SINK EXECUTED SUCCESSFULLY"
  local STREAM_FAILED="E2E: STREAM MODE TEST: STREAMING MODE WITH PARQUET SINK TEST FAILED"
  local BATCH_SUCCESSFUL="E2E: BATCH MODE TEST: BATCH MODE WITH PARQUET SINK EXECUTED SUCCESSFULLY USING ${mode} MODE"
  local BATCH_FAILED="E2E: BATCH MODE TEST: BATCH MODE WITH PARQUET SINK TEST FAILED USING ${mode} MODE"
  print_message "Finding number of patients, encounters and obs in openmrs server"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Patient/$1 2>/dev/null >>./omrs/patients.json
  local total_test_patients=$(jq '.total' ./omrs/patients.json)
  print_message "Total openmrs test patients ---> ${total_test_patients}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Encounter/$2 2>/dev/null >>./omrs/encounters.json
  local total_test_encounters=$(jq '.total' ./omrs/encounters.json)
  print_message "Total openmrs test encounters ---> ${total_test_encounters}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Observation/$3 2>/dev/null >>./omrs/obs.json
  local total_test_obs=$(jq '.total' ./omrs/obs.json)
  print_message "Total openmrs test obs ---> ${total_test_obs}"
  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Patient/ | awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"
  total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Encounter/ | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"
  total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Observation/ | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"  
  if [[ -z "$1" ]]; then
    if [[ ${total_patients_streamed} == ${total_test_patients} 
          && ${total_encounters_streamed} == ${total_test_encounters} 
          && ${total_obs_streamed} == ${total_test_obs} ]] ; then
        echo ${BATCH_SUCCESSFUL}
        cd "${HOME_PATH}"
    else
        echo ${BATCH_FAILED}    
        return 1
    fi 
  else 
    if [[ ${total_patients_streamed} == ${total_test_patients} 
          && ${total_encounters_streamed} == ${total_test_encounters} 
          && ${total_obs_streamed} == ${total_test_obs} ]] ; then
        echo ${STREAM_SUCCESSFUL}
    else    
        echo ${STREAM_FAILED}
        return 1
    fi
   return 1 
  fi        
}

#############################################################################################
# Function that validates fhir counts for batch and stream mode.
# Arguments:
#     $1 The 'Patient' query parameter, e.g., '?family=Clive' or empty to fetch all patients
#############################################################################################
function validate_fhir_counts() {
  mkdir batch_omrs
  mkdir stream_fhir
  mkdir batch_fhir
  mkdir stream_omrs
  if [[ -z "$1" ]]; then
    print_message "Finding number of patients, encounters and obs in openmrs server"
    curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Patient/?_summary=count 2>/dev/null >>./batch_omrs/patients.json
    local total_test_patients=$(jq '.total' ./batch_omrs/patients.json)
    print_message "Total openmrs test patientss ---> ${total_test_patients}"
    print_message "Counting number of patients, encounters and obs sinked to fhir files"
    curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
      http://localhost:8098/fhir/Patient/?_summary=count 2>/dev/null >>./batch_fhir/patients.json
    local total_patients_sinked_fhir=$(jq '.total' ./batch_fhir/patients.json)
    print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"
    if [[ ${total_test_patients} == ${total_patients_sinked_fhir} ]]; then
        print_message "BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${mode} MODE"
        cd "${HOME_PATH}"
    else    
        print_message "BATCH MODE WITH FHIR SERVER SINK TEST FAILED USING ${mode} MODE"
        return 1
    fi 
  else 
    print_message "Finding number of patients, encounters and obs in openmrs server"
    curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Patient/$1 2>/dev/null >>./stream_omrs/patients.json
    local stream_total_test_patients=$(jq '.total' ./stream_omrs/patients.json)
    print_message "Total openmrs test patients ---> ${stream_total_test_patients}"
    print_message "Counting number of patients, encounters and obs sinked to fhir files"
    curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8098/fhir/Patient/$1 2>/dev/null >>./stream_fhir/patients.json
    local total_test_patients_fhir=$(jq '.total' ./stream_fhir/patients.json)
    print_message "Total patients sinked to fhir ---> ${total_test_patients_fhir}"
    if [[ ${stream_total_test_patients} == ${total_test_patients_fhir} ]]; then
        print_message "STREAMING MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY"
    else    
        print_message "STREAMING MODE WITH FHIR SERVER SINK TEST FAILED"
        return 1
    fi
   return 1 
  fi      
}