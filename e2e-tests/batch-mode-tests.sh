#!/usr/bin/env bash

print_prefix="E2E: BATCH MODE TEST:"
echo "${print_prefix} START OF BATCH MODE TEST"
echo "${print_prefix} STARTING OPENMRS SERVER"
docker-compose -f openmrs-compose.yaml up -d --remove-orphans
openmrs_start_wait_time=0
contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 --connect-timeout 5 \
 --max-time 20 http://localhost:8099/openmrs/ws/rest/v1/visittype 2>/dev/null | cut -d ";" -f 1)
until [[ ${contenttype} == "application/json" ]]; do
      sleep 60s;
      echo "${print_prefix} WAITING FOR OPENMRS SERVER TO START"
      contenttype=$(curl -o /dev/null --head -w "%{content_type}\n" -X GET -u admin:Admin123 --connect-timeout 5 \
       --max-time 20 http://localhost:8099/openmrs/ws/rest/v1/visittype 2>/dev/null | cut -d ";" -f 1)
      ((openmrs_start_wait_time+=1))
      if [[ ${openmrs_start_wait_time} == 20 ]]; then
        echo "${print_prefix} TERMINATING TEST AS OPENMRS TOOK TOO LONG TO START"
        exit 1
      fi
done
echo "${print_prefix} OPENMRS SERVER STARTED SUCCESSFULLY"
echo "${print_prefix} BUILDING THE ANALYTICS PROJECT"
test_dir=$(mktemp -d -t analytics_tests__XXXXXX)
echo "${print_prefix} PARQUET FILES WILL BE WRITTEN INTO $test_dir DIRECTORY"
echo "${print_prefix} RUNNING BATCH MODE WITH PARQUET SYNC"
echo "${print_prefix} mvn exec:java -pl batch \
      -Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123
      --searchList=Patient,Encounter,Observation --batchSize=20 --targetParallelism=20 --fileParquetPath='${test_dir}' "
mvn exec:java -pl batch \
 "-Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123  \
      --searchList=Patient,Encounter,Observation --batchSize=20 --targetParallelism=20 --fileParquetPath='${test_dir}'/ "
if [[ -z $(ls "${test_dir}"/Patient) ]]; then
  echo "${print_prefix} DATA NOT SYNCED TO PARQUET FILES"
  exit 1
fi
echo "${print_prefix} PARQUET FILES SYNCED TO ${test_dir​}"
current_path=$(pwd)
echo "${print_prefix} COPYING PARQUET TOOLS JAR FILE TO ROOT"
cp e2e-tests/parquet-tools-1.11.1.jar "${test_dir}"
if ! [[ $(command -v jq) ]]; then
    echo "${print_prefix} COULD NOT INSTALL JQ, INSTALL MANUALLY TO CONTINUE RUNNING THE TEST"
    exit 1
fi
cd "${test_dir}"
mkdir omrs
echo "${print_prefix} Finding number of patients, encounters and obs in openmrs server"
curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ 2>/dev/null >> ./omrs/patients.json
total_test_patients=$(jq '.total' ./omrs/patients.json)
echo "${print_prefix} Total openmrs test patients ---> ${total_test_patients}"
curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Encounter/ 2>/dev/null >> ./omrs/encounters.json
total_test_encounters=$(jq '.total' ./omrs/encounters.json)
echo "${print_prefix} Total openmrs test encounters ---> ${total_test_encounters}"
curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
      http://localhost:8099/openmrs/ws/fhir2/R4/Observation/ 2>/dev/null >> ./omrs/obs.json
total_test_obs=$(jq '.total' ./omrs/obs.json)
echo "${print_prefix} Total openmrs test obs ---> ${total_test_obs}"
echo "${print_prefix} Counting number of patients, encounters and obs sinked to parquet files"
total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Patient/ | awk '{print $3}')
echo "${print_prefix} Total patients synched to parquet ---> ${total_patients_streamed}"
total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Encounter/ | awk '{print $3}')
echo "${print_prefix} Total encounters synched to parquet ---> ${total_encounters_streamed}"
total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Observation/ | awk '{print $3}')
echo "${print_prefix} Total obs synched to parquet ---> ${total_obs_streamed}"
if [[ ${total_patients_streamed} == ${total_test_patients}
      && ${total_encounters_streamed} == ${total_test_encounters}
      && $total_obs_streamed == ${total_test_obs} ]] ;
then
  echo "${print_prefix} BATCH MODE WITH PARQUET SYNC EXECUTED SUCCESSFULLY"
else
  echo "${print_prefix} BATCH MODE WITH PARQUET SYNC TEST FAILED"
  exit 1
fi
cd "${current_path}"
echo "${print_prefix} RUNNING BATCH MODE WITH FHIR SYNC"
echo "${print_prefix} STARTING FHIR SINK SERVER"
docker-compose -f sink-compose.yml up -d
fhir_server_start_wait_time=0
fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi --connect-timeout 5 \
    --max-time 20 http://localhost:8098/fhir/Observation 2>/dev/null)
until [[ ${fhir_server_status_code} -eq 200 ]]; do
      sleep 1s;
      echo "${print_prefix} WAITING FOR FHIR SERVER TO START"
      fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi --connect-timeout 5 \
            --max-time 20 http://localhost:8098/fhir/Observation 2>/dev/null)
      ((fhir_server_start_wait_time+=1))
      if [[ fhir_server_start_wait_time == 10 ]]; then
        echo "${print_prefix} TERMINATING AS FHIR SERVER TOOK TOO LONG TO START"
        exit 1
      fi
done
echo "${print_prefix} FHIR SERVER STARTED SUCCESSFULLY"
echo "${print_prefix} mvn exec:java -pl batch
 -Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123
   --searchList=Patient --batchSize=20 --targetParallelism=20
   --fhirSinkPath=http://localhost:8098/fhir
   --sinkUserName=hapi --sinkPassword=hapi"

mvn exec:java -pl batch \
 "-Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123 \
   --searchList=Patient --batchSize=20 --targetParallelism=20 \
   --fhirSinkPath=http://localhost:8098/fhir \
   --sinkUserName=hapi --sinkPassword=hapi"
cd "${test_dir}"
mkdir fhir
echo "${print_prefix} Counting number of patients, encounters and obs sinked to fhir files"
curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
      http://localhost:8098/fhir/Patient/?_summary=count 2>/dev/null >> ./fhir/patients.json
total_patients_sinked_fhir=$(jq '.total' ./fhir/patients.json)
echo "${print_prefix} Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"
if [[ ${total_patients_sinked_fhir} == ${total_test_patients} ]] ;
then
  echo "${print_prefix} BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY"
else
  echo "${print_prefix} BATCH MODE WITH FHIR SERVER SINK TEST FAILED"
  exit 1
fi
echo "${print_prefix} END!!"