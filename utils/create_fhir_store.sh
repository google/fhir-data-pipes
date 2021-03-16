#!/bin/bash
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

# This creates a GCP FHIR store in the given project/location/dataset and
# sets up BigQuery streaming to a BigQuery dataset with the same dataset name.

if [[ $# -ne 4 ]]; then
  echo "Usage1 $0 gcp-project location dataset-name fhir-store-name"
  echo "Sample: $0 bashir-variant us-central1 test-dataset openmrs-relay"
  exit 1
fi

#######################################
## Function that creates dataset and checks if the dataset already exist
# Arguments:
#   gcloud command to create a dataset
#######################################
function create_dataset() {
  output=`$@ 2>&1`

  code=$?

  if [[ $code -eq 0 ]]; then
    echo "Created dataset"
  else
    # If the dataset already exists, let user know
    if [[ "${output}" == *"already exists"* ]]; then
      echo "Dataset already exists"

    else
      echo "ERROR ${output}" >&2
      exit 1
    fi
  fi
}

echo "Creating GCP Healthcare dataset.."
create_dataset "gcloud healthcare datasets create ${3} --location=${2}"

echo "Creating FHIR Store..."
curl --request POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/fhir+json; charset=utf-8" \
  "https://healthcare.googleapis.com/v1/projects/${1}/locations/${2}/datasets/${3}/fhirStores?fhirStoreId=${4}" \
  --data '{"version":"R4", "enableUpdateCreate":true,"disableReferentialIntegrity":true}' \
  -w '\nFHIR store creation status code: %{http_code}\n'

echo "Creating BQ dataset..."
create_dataset "bq --location=${2} mk --dataset ${1}:${3}"

echo "Setting up BQ streaming..."
curl -X PATCH \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  -H "Content-Type: application/json; charset=utf-8" \
  --data "{
    'streamConfigs': [
      {
        'bigqueryDestination': {
          'datasetUri': 'bq://${1}.${3}',
          'schemaConfig': {
            'schemaType': 'ANALYTICS'
          }
        }
      }
    ]
  }" \
  "https://healthcare.googleapis.com/v1/projects/${1}/locations/${2}/datasets/${3}/fhirStores/${4}?updateMask=streamConfigs" \
  -w '\nFHIR store BQ streaming set up status code: %{http_code}\n'

echo "Complete!!"
