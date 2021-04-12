#!/usr/bin/env bash

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
  
  TEST_DIR_FHIR=$(mktemp -d -t analytics_tests__XXXXXX_FHIRSEARCH)
  TEST_DIR_JDBC=$(mktemp -d -t analytics_tests__XXXXXX_JDBC)
  JDBC_SETTINGS="--jdbcModeEnabled=true --jdbcUrl=jdbc:mysql://localhost:3306/openmrs --dbUser=mysqluser \
                 --dbPassword=mysqlpw --jdbcMaxPoolSize=50 --jdbcDriverClass=com.mysql.cj.jdbc.Driver"
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
  source ./e2e-tests/common.sh
  test

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
  local command=(mvn exec:java -pl batch "-Dexec.args=--resourceList=Patient --batchSize=20  \
  --fhirSinkPath=http://localhost:8098/fhir  --sinkUserName=hapi --sinkPassword=hapi $1")
  local test_dir=$2
  local mode=$3

  print_message " ${command[*]}"
  "${command[@]}"

  cd "${test_dir}"
  mkdir omrs_fhir_sink
  print_message "Finding number of patients, encounters and obs in openmrs server"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ 2>/dev/null >>./omrs_fhir_sink/patients.json
  total_test_patients=$(jq '.total' ./omrs_fhir_sink/patients.json)

  mkdir fhir
  print_message "Counting number of patients, encounters and obs sinked to fhir files"
  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    http://localhost:8098/fhir/Patient/?_summary=count 2>/dev/null >>./fhir/patients.json
  total_patients_sinked_fhir=$(jq '.total' ./fhir/patients.json)
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"
  if [[ ${total_patients_sinked_fhir} == ${total_test_patients} ]]; then
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

# TODO(omarismail) merge so that both FHIR Sink test and Parquet Sink test are one
print_message "---- STARTING FHIR SINK TEST ----"
test_fhir_sink "${JDBC_SETTINGS}" "${TEST_DIR_JDBC}" "JDBC"
test_fhir_sink "" "${TEST_DIR_FHIR}" "FHIR_SEARCH"

print_message "END!!"
