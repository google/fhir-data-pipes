echo "BATCH MODE TEST"
​
echo "STARTING OPENMRS SERVER"
​
docker-compose -f openmrs-compose.yaml up -d
statuscode=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/rest/v1/visittype)
until [ "$statuscode" -eq 200 ]; do
      sleep 20s;
      echo 'WAITING FOR OPENMRS SERVER TO START'
      statuscode=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/rest/v1/visittype)
done
echo 'OPENMRS SERVER STARTED SUCCESSFULLY'
​
echo "BUILD THE ANALYTICS PROJECT"
​
mvn clean install
​
echo "RUN BATCH MODE WITH PARQUET SINK TESTS"

java -cp batch/target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar org.openmrs.analytics.FhirEtl --openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123  --searchList=Patient,Encounter,Observation --batchSize=20 --targetParallelism=20 --fileParquetPath=/tmp/
​
if [ -z "$(ls /tmp/Patient)" ]; then
  echo 'NO PARQUET FILES FOUND IN /tmp/'
  exit 0
fi
​
current_path=`pwd`

echo "COPYING PARQUET TOOLS JAR FILE TO ROOT"

cp parquet-tools-1.11.1.jar ~/
​
cd ~/

echo "Finding number of patients, encounters and obs in openmrs server"
​
mkdir /tmp/omrs
​
curl -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/fhir2/R4/Patient/ >> /tmp/omrs/patients.json
total_test_patients=$(jq '.total' /tmp/omrs/patients.json)
echo "total_test_patients ---> $total_test_patients"
​
curl -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/fhir2/R4/Encounter/ >> /tmp/omrs/encounters.json
total_test_encounters=$(jq '.total' /tmp/omrs/encounters.json)
echo "total_test_encounters ---> $total_test_encounters"
​
curl -L -X GET -u admin:Admin123 http://localhost:8099/openmrs/ws/fhir2/R4/Observation/ >> /tmp/omrs/obs.json
total_test_obs=$(jq '.total' /tmp/omrs/obs.json)
echo "total_test_obs ---> $total_test_obs"
​
sudo rm -rf /tmp/omrs
​
echo "Counting number of patients, encounters and obs sinked to parquet files"
​
total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount /tmp/Patient/)
echo "total_patients_streamed ---> $total_patients_streamed"
​
total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount /tmp/Encounter/)
echo "total_encounters_streamed ---> $total_encounters_streamed"
​
total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount /tmp/Observation/)
echo "total_obs_streamed ---> $total_obs_streamed"
​
if [[ ($total_patients_streamed == *$total_test_patients*) && ($total_encounters_streamed == *$total_test_encounters*) && ($total_obs_streamed == *$total_obs_encounters*)]] ;
then
  echo "BATCH MODE WITH PARQUET SINK EXECUTED SUCCESSFULLY"
else
  echo "BATCH MODE WITH PARQUET SINK TEST FAILED"
fi
​
​
cd $current_path
​
echo "BATCH MODE WITH FHIR SINK TESTS"
​
echo "STARTING FHIR SERVER"
​
docker-compose -f sink-compose.yml up -d
fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi http://localhost:8098/fhir/Observation)
until [ "$fhir_server_status_code" -eq 200 ]; do
      sleep 20s;
      echo 'WAITING FOR FHIR SERVER TO START'
      fhir_server_status_code=$(curl -o /dev/null --head -w "%{http_code}" -L -X GET -u hapi:hapi http://localhost:8098/fhir/Observation)
done
echo 'FHIR SERVER STARTED SUCCESSFULLY'
​
mvn exec:java -pl batch \
 "-Dexec.args=--openmrsServerUrl=http://localhost:8099/openmrs --openmrsUserName=admin --openmrsPassword=Admin123 \
   --searchList=Patient --batchSize=20 --targetParallelism=20 \
   --fhirSinkPath=http://localhost:8098/fhir \
   --sinkUserName=hapi --sinkPassword=hapi"

mkdir /tmp/fhir
​
curl -L -X GET -u hapi:hapi http://localhost:8098/fhir/Patient/ >> /tmp/fhir/patients.json
total_patients_sinked_fhir=$(jq '.total' /tmp/fhir/patients.json)
echo "total_patients_sinked_fhir ---> $total_patients_sinked_fhir"
​
sudo rm -rf /tmp/fhir
​
if [[ ($total_patients_sinked_fhir == $total_test_patients)]] ;
then
  echo "BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY"
else
  echo "BATCH MODE WITH FHIR SERVER SINK TEST FAILED"
fi
​
echo "END OF BATCH MODE TEST"