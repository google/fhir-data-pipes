#!/usr/bin/env bash

set -e

function setup() {  
  OPENMRS_URL='http://localhost:8099'
  SINK_SERVER='http://localhost:8098'

  if [[ $1 = "--use_docker_network" ]]; then
    OPENMRS_URL='http://openmrs:8080'
    SINK_SERVER='http://sink-server:8080'
  fi
}

function openmrs_check() {
  openmrs_start_wait_time=0
  contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 \
      --connect-timeout 5 --max-time 20 ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Patient \
      2>/dev/null | cut -d ";" -f 1)
  until [[ ${contenttype} == "application/fhir+json" ]]; do
    sleep 60s
    echo "WAITING FOR OPENMRS SERVER TO START"
    contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 \
      --connect-timeout 5 --max-time 20 ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Patient \
      2>/dev/null | cut -d ";" -f 1)
    ((openmrs_start_wait_time += 1))
    if [[ ${openmrs_start_wait_time} == 20 ]]; then
      echo "TERMINATING TEST AS OPENMRS TOOK TOO LONG TO START"
      exit 1
    fi
  done
  echo "OPENMRS SERVER STARTED SUCCESSFULLY"
}

function fhir_check() {
  fhir_server_start_wait_time=0
  fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
  -u hapi:hapi --connect-timeout 5 --max-time 20 \
  ${SINK_SERVER}/fhir/Observation 2>/dev/null)
  until [[ ${fhir_server_status_code} -eq 200 ]]; do
    sleep 1s
    echo "WAITING FOR FHIR SERVER TO START"
    fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET \
      -u hapi:hapi --connect-timeout 5 --max-time 20 \
      ${SINK_SERVER}/fhir/Observation 2>/dev/null)
    ((fhir_server_start_wait_time += 1))
    if [[ fhir_server_start_wait_time == 10 ]]; then
      echo "TERMINATING AS FHIR SERVER TOOK TOO LONG TO START"
      exit 1
    fi
  done
  echo "FHIR SERVER STARTED SUCCESSFULLY"
}

setup $1
openmrs_check
fhir_check