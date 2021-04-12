#!/usr/bin/env bash

#######################################
# Function that sets up testing env
#######################################
function setup() {
  HOME_PATH=$(pwd)

  echo "BUILDING THE ANALYTICS PROJECT"
  mvn compile
  docker-compose -f openmrs-compose.yaml up -d --remove-orphans
  sleep 2m;
  openmrs_start_wait_time=0
  contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 --connect-timeout 5 \
   --max-time 20 http://localhost:8099/openmrs/ws/rest/v1/visittype 2>/dev/null | cut -d ";" -f 1)
   until [[ ${contenttype} == "application/json" ]]; do
      sleep 60s;
      echo "WAITING FOR OPENMRS SERVER TO START"
      contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 --connect-timeout 5 \
       --max-time 20 http://localhost:8099/openmrs/ws/rest/v1/visittype 2>/dev/null | cut -d ";" -f 1)
      ((openmrs_start_wait_time+=1))
      if [[ ${openmrs_start_wait_time} == 20 ]]; then
        echo "TERMINATING TEST AS OPENMRS TOOK TOO LONG TO START"
        exit 1
      fi
   done
  echo "OPENMRS SERVER STARTED SUCCESSFULLY"

  cd "${current_path}"
  echo "STARTING FHIR SINK SERVER"
  docker-compose -f sink-compose.yml up -d
  fhir_server_start_wait_time=0
  fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi --connect-timeout 5 \
    --max-time 20 http://localhost:8098/fhir/Observation 2>/dev/null)
  until [[ ${fhir_server_status_code} -eq 200 ]]; do
      sleep 1s;
      echo "WAITING FOR FHIR SERVER TO START"
      fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi --connect-timeout 5 \
            --max-time 20 http://localhost:8098/fhir/Observation 2>/dev/null)
      ((fhir_server_start_wait_time+=1))
      if [[ fhir_server_start_wait_time == 10 ]]; then
        echo "TERMINATING AS FHIR SERVER TOOK TOO LONG TO START"
        exit 1
      fi
  done
  echo "FHIR SERVER STARTED SUCCESSFULLY"
}

function test() {
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
 

