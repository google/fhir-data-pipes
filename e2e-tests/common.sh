#!/usr/bin/env bash

#######################################
# Function that sets up testing env
#######################################
function setup() {
  HOME_PATH=$(pwd)

  print_message "BUILDING THE ANALYTICS PROJECT"
  mvn compile

  print_message "STARTING SERVERs"
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
  JDBC_SETTINGS="--jdbcModeEnabled=true --jdbcUrl=jdbc:mysql://localhost:3306/openmrs --dbUser=mysqluser \
                 --dbPassword=mysqlpw --jdbcMaxPoolSize=50 --jdbcDriverClass=com.mysql.cj.jdbc.Driver"
}

function validate_counts() {
  mkdir omrs
  print_message "Finding number of patients, encounters and obs in openmrs server"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
  http://localhost:8099/openmrs/ws/fhir2/R4/Patient/$1 2>/dev/null >>./omrs/patients.json
  total_test_patients=$(jq '.total' ./omrs/patients.json)
  print_message "Total openmrs test patients ---> ${total_test_patients}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
  http://localhost:8099/openmrs/ws/fhir2/R4/Encounter/$2 2>/dev/null >>./omrs/encounters.json
  total_test_encounters=$(jq '.total' ./omrs/encounters.json)
  print_message "Total openmrs test encounters ---> ${total_test_encounters}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
  http://localhost:8099/openmrs/ws/fhir2/R4/Observation/$3 2>/dev/null >>./omrs/obs.json
  total_test_obs=$(jq '.total' ./omrs/obs.json)
  print_message "Total openmrs test obs ---> ${total_test_obs}"
  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Patient/ | awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"
  total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Encounter/ | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"
  total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Observation/ | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"
}