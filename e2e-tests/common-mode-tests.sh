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
