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
# Function that prints messages
# Arguments:
#   anything that needs printing.
#######################################
function print_message() {
  local print_prefix="E2E: STREAM MODE TEST:"
  echo "${print_prefix} $*"
}

#######################################
# Call common.sh to setup 
# the testing env
#  
#######################################
source ./e2e-tests/common-mode-tests.sh
setup
test_dir=$(mktemp -d -t analytics_tests__XXXXXX_streaming)
command=(mvn compile exec:java -pl streaming-binlog \
   -Dexec.args="--openmrsUserName=admin --openmrsPassword=Admin123 \
   --openmrsServerUrl=http://localhost:8099/openmrs \
   --openmrsFhirBaseEndpoint=/ws/fhir2/R4 \
   --outputParquetPath='${test_dir}' --fhirSinkPath=http://localhost:8098/fhir --sinkUserName=hapi --sinkPassword=hapi123 ")

print_message "PARQUET FILES WILL BE WRITTEN INTO $test_dir DIRECTORY"
print_message "BUILDING THE ANALYTICS  PIPELINE"
timeout 1m "${command[@]}"
print_message "CREATE A PATIENT IN OPENMRS TO BE STREAMED"
curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/fhir2/R4/Patient -d @./e2e-tests/patient.json
print_message "CREATE AN ENCOUNTER IN OPENMRS TO BE STREAMED"
curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/rest/v1/encounter -d @./e2e-tests/encounter.json
print_message "CREATE AN OBSERVATION IN OPENMRS TO BE STREAMED"
curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/fhir2/R4/Observation -d @./e2e-tests/obs.json
print_message "RUNNING STREAMING MODE WITH PARQUET & FHIR SYNC"
print_message "${command[@]}"
timeout 2m "${command[@]}"
if [[ -z $(ls "${test_dir}"/Patient) ]]; then
   print_message "DATA NOT SYNCED TO PARQUET FILES"
  exit 1
fi
print_message "PARQUET FILES WRITTEN TO ${test_dir​}"
current_path=$(pwd)
print_message "COPYING PARQUET TOOLS JAR FILE TO ROOT"
cp e2e-tests/parquet-tools-1.11.1.jar "${test_dir}"
if ! [[ $(command -v jq) ]]; then
    print_message "COULD NOT INSTALL JQ, INSTALL MANUALLY TO CONTINUE RUNNING THE TEST"
    exit 1
fi
cd "${test_dir}"
validate_parquet_counts "?family=Clive" "?subject.given=Cliff" "?subject.given=Cliff"
validate_fhir_counts "?family=Clive"
print_message "END!!"
