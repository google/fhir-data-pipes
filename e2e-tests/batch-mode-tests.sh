#!/usr/bin/env bash

set -e

#######################################
# Function that prints messages
# Arguments:
#   anything that needs printing
#######################################
function print_message() {
  local print_prefix="E2E: BATCH MODE TEST:"
  echo "${print_prefix} $*"
}

#######################################
# Function that sets up e2e test
# Arguments:
#   Path where e2e-tests directory is. 
#     Directory contains parquet tools jar
#     as well as subdirectory of parquet file output
#   Subdirectory to search for parquet files. 
#      NON_JDBC or JDBC
#   Optional: Flag to specify whether test is being 
#      done on docker network.
#######################################
function setup() {
  HOME_PATH=$1
  rm -rf ${HOME_PATH}/fhir
  rm -rf ${HOME_PATH}/$2/*.json
  
  OPENMRS_URL='http://localhost:8099'
  SINK_SERVER='http://localhost:8098'

  if [[ $3 = "--use_docker_network" ]]; then
    OPENMRS_URL='http://openmrs:8080'
    SINK_SERVER='http://sink-server:8080'
  fi
}

#######################################
# Function to use count resources in openmrs server
# Globals:
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
# Arguments:
#   directory to sink files to
#######################################
function openmrs_query() {

  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Patient/ 2>/dev/null >>${HOME_PATH}/$1/patients.json
  TOTAL_TEST_PATIENTS=$(jq '.total' ${HOME_PATH}/$1/patients.json)
  print_message "Total openmrs test patients ---> ${TOTAL_TEST_PATIENTS}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Encounter/ 2>/dev/null >>${HOME_PATH}/$1/encounters.json
  TOTAL_TEST_ENCOUNTERS=$(jq '.total' ${HOME_PATH}/$1/encounters.json)
  print_message "Total openmrs test encounters ---> ${TOTAL_TEST_ENCOUNTERS}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Observation/ 2>/dev/null >>${HOME_PATH}/$1/obs.json
  TOTAL_TEST_OBS=$(jq '.total' ${HOME_PATH}/$1/obs.json)
  print_message "Total openmrs test obs ---> ${TOTAL_TEST_OBS}"
}


#######################################
# Function that tests sinking to parquet files
# and compares output to what is in openmrs server
# Arguments:
#   the mode to test: FHIR Search vs JDBC
#######################################
function test_parquet_sink() {
  local test_dir=$1
  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  
  total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ${HOME_PATH}/${test_dir}/Patient/ | awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"
  total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ${HOME_PATH}/${test_dir}/Encounter/ | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"
  total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ${HOME_PATH}/${test_dir}/Observation/ | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"

  if [[ ${total_patients_streamed} == ${TOTAL_TEST_PATIENTS} && ${total_encounters_streamed} \
        == ${TOTAL_TEST_ENCOUNTERS} && ${total_obs_streamed} == ${TOTAL_TEST_OBS} ]] \
    ; then
    print_message "BATCH MODE WITH PARQUET SINK EXECUTED SUCCESSFULLY USING ${test_dir} MODE"
  else
    print_message "BATCH MODE WITH PARQUET SINK TEST FAILED USING ${test_dir} MODE"
    exit 1
  fi
}

#######################################
# Function that tests sinking to FHIR server
# and compares output to what is in openmrs server
# Arguments:
#   the mode to test: FHIR Search vs JDBC
#######################################
function test_fhir_sink() {
  local mode=$1

  print_message "Finding number of patients, encounters and obs in FHIR server"

  mkdir ${HOME_PATH}/fhir
  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    ${SINK_SERVER}/fhir/Patient/?_summary=count 2>/dev/null >>${HOME_PATH}/fhir/patients.json

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    ${SINK_SERVER}/fhir/Encounter/?_summary=count 2>/dev/null >>${HOME_PATH}/fhir/encounters.json

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    ${SINK_SERVER}/fhir/Observation/?_summary=count 2>/dev/null >>${HOME_PATH}/fhir/obs.json

  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  total_patients_sinked_fhir=$(jq '.total' ${HOME_PATH}/fhir/patients.json)
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  total_encounters_sinked_fhir=$(jq '.total' ${HOME_PATH}/fhir/encounters.json)
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  total_obs_sinked_fhir=$(jq '.total' ${HOME_PATH}/fhir/obs.json)
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ ${total_patients_sinked_fhir} == ${TOTAL_TEST_PATIENTS} && ${total_encounters_sinked_fhir} \
        == ${TOTAL_TEST_ENCOUNTERS} && ${total_obs_sinked_fhir} == ${TOTAL_TEST_OBS} ]] \
    ; then
    print_message "BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${mode} MODE"
  else
    print_message "BATCH MODE WITH FHIR SERVER SINK TEST FAILED USING ${mode} MODE"
    exit 1
  fi
}

setup $1 $2 $3
print_message "---- STARTING $2 TEST ----"
openmrs_query $2
test_parquet_sink $2
test_fhir_sink $2
print_message "END!!"