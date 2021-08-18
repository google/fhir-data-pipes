#!/usr/bin/env bash

# TODO add
# set -e

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
# Function that sets up testing env
#######################################
function setup() {
  if [[ -z "${ROOT_PATH}" ]]; then
    echo "ERROR: environment variable ROOT_PATH not set!"
    exit 1
  fi
  cd ${ROOT_PATH}/pipelines
  HOME_PATH=$(pwd)

  print_message "BUILDING THE ANALYTICS PROJECT"
  mvn compile

  print_message "STARTING SERVERs"
  docker-compose -f ${ROOT_PATH}/docker/openmrs-compose.yaml up -d --remove-orphans
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

  docker-compose -f ${ROOT_PATH}/docker/sink-compose.yml up -d
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


#######################################
# Function to use count resources in openmrs server
# Arguments:
#   directory to sink files to
#######################################
function openmrs_query() {
  mkdir $1

  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ 2>/dev/null >>./$1/patients.json
  total_test_patients=$(jq '.total' ./$1/patients.json)
  print_message "Total openmrs test patients ---> ${total_test_patients}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Encounter/ 2>/dev/null >>./$1/encounters.json
  total_test_encounters=$(jq '.total' ./$1/encounters.json)
  print_message "Total openmrs test encounters ---> ${total_test_encounters}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Observation/ 2>/dev/null >>./$1/obs.json
  total_test_obs=$(jq '.total' ./$1/obs.json)
  print_message "Total openmrs test obs ---> ${total_test_obs}"
}


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
  cp ${ROOT_PATH}/e2e-tests/parquet-tools-1.11.1.jar "${test_dir}"
  if ! [[ $(command -v jq) ]]; then
    print_message "COULD NOT INSTALL JQ, INSTALL MANUALLY TO CONTINUE RUNNING THE TEST"
    exit 1
  fi
  cd "${test_dir}"
  print_message "Finding number of patients, encounters and obs in openmrs server"
  openmrs_query omrs
  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Patient/ | awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"
  total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Encounter/ | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"
  total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Observation/ | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"

  if [[ ${total_patients_streamed} == ${total_test_patients} && ${total_encounters_streamed} == ${total_test_encounters} && ${total_obs_streamed} == ${total_test_obs} ]] \
    ; then
    print_message "BATCH MODE WITH PARQUET SINK EXECUTED SUCCESSFULLY USING ${mode} MODE"
    cd "${HOME_PATH}"
  else
    print_message "BATCH MODE WITH PARQUET SINK TEST FAILED USING ${mode} MODE"
    exit 1
  fi
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
  local command=(mvn exec:java -pl batch "-Dexec.args=--resourceList=Patient,Encounter,Observation --batchSize=20  \
  --fhirSinkPath=http://localhost:8098/fhir  --sinkUserName=hapi --sinkPassword=hapi $1")
  local test_dir=$2
  local mode=$3

  print_message " ${command[*]}"
  "${command[@]}"

  cd "${test_dir}"
  print_message "Finding number of patients, encounters and obs in openmrs server"
  openmrs_query omrs_fhir_sink

  mkdir fhir
  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    http://localhost:8098/fhir/Patient/?_summary=count 2>/dev/null >>./fhir/patients.json

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    http://localhost:8098/fhir/Encounter/?_summary=count 2>/dev/null >>./fhir/encounters.json

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    http://localhost:8098/fhir/Observation/?_summary=count 2>/dev/null >>./fhir/obs.json


  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  total_patients_sinked_fhir=$(jq '.total' ./fhir/patients.json)
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  total_encounters_sinked_fhir=$(jq '.total' ./fhir/encounters.json)
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  total_obs_sinked_fhir=$(jq '.total' ./fhir/obs.json)
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ ${total_patients_sinked_fhir} == ${total_test_patients} && ${total_encounters_sinked_fhir} == ${total_test_encounters} && ${total_obs_sinked_fhir} == ${total_test_obs} ]] \
    ; then
    print_message "BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${mode} MODE"
    cd "${HOME_PATH}"
  else
    print_message "BATCH MODE WITH FHIR SERVER SINK TEST FAILED USING ${mode} MODE"
    exit 1
  fi
}

setup
print_message "---- STARTING PARQUET SINK TEST ----"
test_parquet_sink "--outputParquetPath=${TEST_DIR_FHIR}/" "${TEST_DIR_FHIR}" "FHIR_SEARCH"
test_parquet_sink "--outputParquetPath=${TEST_DIR_JDBC}/ ${JDBC_SETTINGS}" "${TEST_DIR_JDBC}" "JDBC"

print_message "---- STARTING FHIR SINK TEST ----"
test_fhir_sink "" "${TEST_DIR_FHIR}" "FHIR_SEARCH"
test_fhir_sink "${JDBC_SETTINGS}" "${TEST_DIR_JDBC}" "JDBC"

print_message "END!!"
