#!/bin/bash
print_prefix="E2E: BATCH MODE TEST:"
echo "START OF BATCH MODE TEST"
echo "${print_prefix} STARTING OPENMRS SERVER"
docker-compose -f openmrs-compose.yaml up -d
statuscode=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/rest/v1/visittype)
until [[ ${statuscode} -eq 200 ]]; do
      sleep 40s;
      echo "${print_prefix} WAITING FOR OPENMRS SERVER TO START"
      statuscode=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/rest/v1/visittype)
done
echo "${print_prefix} OPENMRS SERVER STARTED SUCCESSFULLY"
echo "${print_prefix} BUILD THE ANALYTICS PROJECT"
mvn clean install
rm -rf /tmp/analytics-tests
mktemp -d /tmp/analytics-tests
echo "${print_prefix} RUN BATCH MODE WITH PARQUET SYNC"
java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar org.openmrs.analytics.FhirEtl --openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123  --searchList=Patient,Encounter,Observation --batchSize=20 --targetParallelism=20 --fileParquetPath=/tmp/analytics-tests/
if [[ -z $(ls /tmp/analytics-tests/Patient) ]]; then
  echo "${print_prefix} DATA NOT SYNCED TO PARQUET FILES"
  exit 0
fi
echo "${print_prefix} PARQUET FILES SYNCED IN /tmp/analytics-tests DIRECTORY"​
current_path=`pwd`
echo "${print_prefix} COPYING PARQUET TOOLS JAR FILE TO ROOT"
cp e2e-tests/parquet-tools-1.11.1.jar /tmp/analytics-tests/
if ! command -v jq &> /dev/null; then #add !
    echo "${print_prefix} INSTALLING JSON COMMAND-LINE PROCESS, JQ"
    sudo apt install jq
fi
if ! command -v jq &> /dev/null; then #add !
    echo "${print_prefix} JQ COULD NOT BE INSTALLED, INSTALL JQ MANUALLY TO CONTINUE RUNNING THE TESTS"
    exit 0
fi
cd /tmp/analytics-tests
mkdir omrs
echo "${print_prefix} Finding number of patients, encounters and obs in openmrs server"
curl -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ >> ./omrs/patients.json
total_test_patients=$(jq '.total' ./omrs/patients.json)
echo "${print_prefix} Total openmrs test patients ---> ${total_test_patients}"
curl -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/fhir2/R4/Encounter/ >> ./omrs/encounters.json
total_test_encounters=$(jq '.total' ./omrs/encounters.json)
echo "${print_prefix} Total openmrs test encounters ---> ${total_test_encounters}"
curl -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/fhir2/R4/Observation/ >> ./omrs/obs.json
total_test_obs=$(jq '.total' ./omrs/obs.json)
echo "${print_prefix} Total openmrs test obs ---> ${total_test_obs}"
echo "${print_prefix} Counting number of patients, encounters and obs sinked to parquet files"
total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Patient/)
echo "${print_prefix} Total patients synched to parquet ---> ${total_patients_streamed}"
total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Encounter/)
echo "${print_prefix} Total encounters synched to parquet ---> ${total_encounters_streamed}"
total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ./Observation/)
echo "${print_prefix} Total obs synched to parquet ---> ${total_obs_streamed}"
if [[ ($total_patients_streamed == *$total_test_patients*) && ($total_encounters_streamed == *$total_test_encounters*) && ($total_obs_streamed == *$total_obs_encounters*)]] ;
then
  echo "${print_prefix} BATCH MODE WITH PARQUET SYNC EXECUTED SUCCESSFULLY"
else
  echo "${print_prefix} BATCH MODE WITH PARQUET SYNC TEST FAILED"
fi
cd $current_path
echo "RUN BATCH MODE WITH FHIR SYNC"
echo "STARTING FHIR SERVER"
docker-compose -f sink-compose.yml up -d
fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi http://localhost:8098/fhir/Observation)
echo "fhir_server_status_code ---> ${fhir_server_status_code}"
until [[ ${fhir_server_status_code} -eq 200 ]]; do
      sleep 20s;
      echo 'WAITING FOR FHIR SERVER TO START'
      fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi http://localhost:8098/fhir/Observation)
done
echo "${print_prefix} FHIR SERVER STARTED SUCCESSFULLY"
mvn exec:java -pl batch \
 "-Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123 \
   --searchList=Patient --batchSize=20 --targetParallelism=20 \
   --fhirSinkPath=http://localhost:8098/fhir \
   --sinkUserName=hapi --sinkPassword=hapi"

cd /tmp/analytics-tests
mkdir fhir
echo "${print_prefix} Counting number of patients, encounters and obs sinked to fhir files"
curl -L -X GET -u hapi:hapi http://localhost:8098/fhir/Patient/ >> ./fhir/patients.json
total_patients_sinked_fhir=$(jq '.total' ./fhir/patients.json)
echo "Total patients sinked to fhir ---> $total_patients_sinked_fhir"
if [[ ($total_patients_sinked_fhir == $total_test_patients)]] ;
then
  echo "${print_prefix} BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY"
else
  echo "${print_prefix} BATCH MODE WITH FHIR SERVER SINK TEST FAILED"
fi
echo "${print_prefix} END!!"