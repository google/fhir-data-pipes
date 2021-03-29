#!/usr/bin/env bash

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
# Call common-mode-tests to setup 
# the testing env
#  
#######################################
source ./e2e-tests/common-mode-tests.sh
setup

print_message "OPENMRS AND FHIR SERVER STARTED SUCCESSFULLY"

test_dir=$(mktemp -d -t analytics_tests__XXXXXX)

print_message "PARQUET FILES WILL BE WRITTEN INTO $test_dir DIRECTORY"

print_message "BUILDING THE ANALYTICS  PIPELINE"

timeout 1m mvn compile exec:java -pl streaming-binlog \
   -Dexec.args="--openmrsUserName=admin --openmrsPassword=Admin123 \
   --openmrsServerUrl=http://localhost:8099/openmrs \
   --openmrsfhirBaseEndpoint=/ws/fhir2/R4 \
   --outputParquetPath='${test_dir}' --fhirSinkPath=http://localhost:8098/fhir --sinkUserName=hapi --sinkPassword=hai "


print_message "SEARCH INDEX REBUILD"

curl -i -u admin:Admin123 -X POST http://localhost:8099/openmrs/ws/rest/v1/searchindexupdate

print_message "CREATE A PATIENT IN OPENMRS TO BE STREAMED"

curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/fhir2/R4/Patient -d @./utils/patient.json

print_message "CREATE AN ENCOUNTER IN OPENMRS TO BE STREAMED"

curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/rest/v1/encounter -d @./utils/encounter.json

print_message "CREATE AN OBSERVATION IN OPENMRS TO BE STREAMED"

curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/fhir2/R4/Observation -d @./utils/obs.json

 print_message "RUNNING STREAM MODE WITH PARQUET & FHIR SYNC"
 print_message "mvn compile exec:java -pl streaming-binlog \
   -Dexec.args= --openmrsUserName=admin --openmrsPassword=Admin123 \
   --openmrsServerUrl=http://localhost:8099/openmrs \
   --openmrsfhirBaseEndpoint=/ws/fhir2/R4 \
   --fileParquetPath='${test_dir}' --fhirSinkPath=http://localhost:8098/fhir --sinkUserName=hapi --sinkPassword=hap "

timeout 2m mvn compile exec:java -pl streaming-binlog \
   -Dexec.args="--openmrsUserName=admin --openmrsPassword=Admin123 \
   --openmrsServerUrl=http://localhost:8099/openmrs \
   --openmrsfhirBaseEndpoint=/ws/fhir2/R4 \
   --outputParquetPath='${test_dir}' --fhirSinkPath=http://localhost:8098/fhir --sinkUserName=hapi --sinkPassword=hap "



if [[ -z $(ls "${test_dir}"/Patient) ]]; then
   print_message "DATA NOT SYNCED TO PARQUET FILES"
  exit 1
fi
 print_message "PARQUET FILES SYNCED TO ${test_dir​}"
current_path=$(pwd)
 print_message "COPYING PARQUET TOOLS JAR FILE TO ROOT"
cp e2e-tests/parquet-tools-1.11.1.jar "${test_dir}"
if ! [[ $(command -v jq) ]]; then
    print_message "COULD NOT INSTALL JQ, INSTALL MANUALLY TO CONTINUE RUNNING THE TEST"
    exit 1
fi
cd "${test_dir}"
mkdir omrs
print_message "Finding number of patients, encounters and obs in openmrs server"
curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Patient?family=Clive 2>/dev/null >> ./omrs/patients.json
total_test_patients=$(jq '.total' ./omrs/patients.json)
print_message "Total openmrs test patients ---> ${total_test_patients}"
curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Encounter?subject.given=Cliff 2>/dev/null >> ./omrs/encounters.json
total_test_encounters=$(jq '.total' ./omrs/encounters.json)
print_message "Total openmrs test encounters ---> ${total_test_encounters}"
curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Observation?subject.given=Cliff 2>/dev/null >> ./omrs/obs.json
total_test_obs=$(jq '.total' ./omrs/obs.json)
print_message "Total openmrs test obs ---> ${total_test_obs}"
print_message "Counting number of patients, encounters and obs sinked to parquet files"
total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Patient/ | awk '{print $3}')
print_message "Total patients synched to parquet ---> ${total_patients_streamed}"
total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Encounter/ | awk '{print $3}')
print_message "Total encounters synched to parquet ---> ${total_encounters_streamed}"
total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Observation/ | awk '{print $3}')
print_message "Total obs synched to parquet ---> ${total_obs_streamed}"

if [[ ${total_patients_streamed} == ${total_test_patients}
      && ${total_encounters_streamed} == ${total_test_encounters}
      && $total_obs_streamed == ${total_test_obs} ]] ;
then
  print_message "STREAM MODE WITH PARQUET SYNC EXECUTED SUCCESSFULLY"
else
  print_message "STREAM MODE WITH PARQUET SYNC TEST FAILED"
  exit 1
fi

cd "${test_dir}"
mkdir fhir
print_message "Counting number of patients, encounters and obs sinked to fhir files"
curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
      http://localhost:8098/fhir/Patient?family=Clive 2>/dev/null >> ./fhir/patients.json
total_patients_sinked_fhir=$(jq '.total' ./fhir/patients.json)

print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

if [[ ${total_patients_sinked_fhir} == ${total_test_patients} ]] ;
then
  print_message "STREAM MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY"
else
  print_message "STREAM MODE WITH FHIR SERVER SINK TEST FAILED"
  exit 1
fi
print_message "END!!"
