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

curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/fhir2/R4/Patient -d '{ 
    "resourceType": "Patient",
    "text": {
        "status": "generated",
        "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\"><table class=\"hapiPropertyTable\"><tbody><tr><td>Id:</td><td>1e7e9782-2e97-44a0-ab2e-9d04498d4ca6</td></tr><tr><td>Identifier:</td><td><div>10000X</div></td></tr><tr><td>Active:</td><td>true</td></tr><tr><td>Name:</td><td> Paul <b>WALKER </b></td></tr><tr><td>Gender:</td><td>MALE</td></tr><tr><td>Birth Date:</td><td>11/04/1971</td></tr><tr><td>Deceased:</td><td>false</td></tr><tr><td>Address:</td><td><span>City7001 </span><span>State7001 </span><span>Country7001 </span></td></tr></tbody></table></div>"
    },
    "identifier": [
        {
            "id": "05a29f94-c0ed-11e2-94be-8c13b969e334",
            "extension": [
                {
                    "url": "http://fhir.openmrs.org/ext/patient/identifier#location",
                    "valueReference": {
                        "reference": "Location/8d6c993e-c2cc-11de-8d13-0010c6dffd0f",
                        "type": "Location",
                        "display": "Unknown Location"
                    }
                }
            ],
            "use": "official",
            "type": {
                "text": "OpenMRS ID"
            },
            "value": "10002T"
        }
    ],
    "active": true,
    "name": [
        {
            "id": "9d550771-11d6-41b6-89c1-893ecd78210b",
            "family": "Clive",
            "given": [
                "Jita"
            ]
        }
    ],
    "gender": "male",
    "birthDate": "1971-04-11",
    "deceasedBoolean": false,
    "address": [
        {
            "id": "f4ba90db-e412-4aa3-a519-2c33eac3e867",
            "extension": [
                {
                    "url": "http://fhir.openmrs.org/ext/address",
                    "extension": [
                        {
                            "url": "http://fhir.openmrs.org/ext/address#address1",
                            "valueString": "Address17001"
                        }
                    ]
                }
            ],
            "use": "home",
            "city": "City7001",
            "state": "State7001",
            "postalCode": "47002",
            "country": "Country7001"
        }
    ]
}'

print_message "CREATE AN ENCOUNTER IN OPENMRS TO BE STREAMED"

curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/rest/v1/encounter -d '{
"encounterDatetime":"2020-09-16T14:08:40.000+0530",
"patient":"957d5571-dee2-4e5e-8ece-b65d2d965248",
"location":"aff27d58-a15c-49a6-9beb-d30dcfc0c66e",
"encounterType":"e22e39fd-7db2-45e7-80f1-60fa0d5a4378",
"encounterProviders": [
    {
      "provider": "fb6adae0-191d-4d5f-9c7a-54af74270c67",
      "encounterRole": "240b26f9-dd88-4172-823d-4a8bfeb7841f"
    }
  ]
}'

print_message "CREATE AN OBSERVATION IN OPENMRS TO BE STREAMED"

curl -i -u admin:Admin123 -H "Content-type: application/json" -X POST http://localhost:8099/openmrs/ws/fhir2/R4/Observation -d '{

    "resourceType": "Observation",
    "text": {
        "status": "generated",
        "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\"><table class=\"hapiPropertyTable\"><tbody><tr><td>Id:</td><td>019d49b0-32a8-4c63-9de4-5f3aaee32910</td></tr><tr><td>Status:</td><td>AMENDED</td></tr><tr><td>Category:</td><td> Exam </td></tr><tr><td>Code:</td><td> Diagnosis order </td></tr><tr><td>Subject:</td><td><a href=\"http://localhost:8080/openmrs/ws/fhir2/R4/Patient/637d5571-dee2-4e5e-8ece-b65d2d965081\">Mary Thomas (OpenMRS ID: 10001V)</a></td></tr><tr><td>Encounter:</td><td><a href=\"http://localhost:8080/openmrs/ws/fhir2/R4/Encounter/e218d239-9eba-40c1-9dcf-d5cb69b3253e\">Encounter/e218d239-9eba-40c1-9dcf-d5cb69b3253e</a></td></tr><tr><td>Effective:</td><td> 27 September 2020 11:13:01 </td></tr><tr><td>Issued:</td><td>14/01/2021 12:23:02 PM</td></tr><tr><td>Value:</td><td> Primary </td></tr></tbody></table></div>"
    },
    "status": "amended",
    "category": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "exam",
                    "display": "Exam"
                }
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "code": "159946AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "display": "Diagnosis order"
            }
        ]
    },
    "subject": {
        "reference": "Patient/957d5571-dee2-4e5e-8ece-b65d2d965248",
        "type": "Patient",
        "display": "Cliff Gita (OpenMRS ID: 10003P)"
    },
    "encounter": {
        "reference": "Encounter/e218d239-9eba-40c1-9dcf-d5cb69b3253e",
        "type": "Encounter"
    },
    "effectiveDateTime": "2020-09-27T11:13:01+00:00",
    "issued": "2021-01-14T12:23:02.000+00:00",
    "valueCodeableConcept": {
        "coding": [
            {
                "code": "159943AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "display": "Primary"
            }
        ]
    }
}'

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
