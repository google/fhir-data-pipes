#!/bin/bash
# This creates a GCP FHIR store in the given project/location/dataset and
# sets up BigQuery streaming to a BigQuery dataset with the same dataset name.

if [[ $# -ne 4 ]]; then
  echo "Usage1 $0 gcp-project location dataset-name fhir-store-name"
  echo "Sample: $0 bashir-variant us-central1 test-dataset openmrs-relay"
  exit 1
fi

# Creating FHIR store
curl --request POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/fhir+json; charset=utf-8" \
  "https://healthcare.googleapis.com/v1/projects/${1}/locations/${2}/datasets/${3}/fhirStores?fhirStoreId=${4}" \
  --data '{"version":"R4", "enableUpdateCreate":true,"disableReferentialIntegrity":true}' \
  -w '\nFHIR store creation status code: %{http_code}\n'

# TODO create BQ dataset instead of assuming it exists.

# Setting up BQ streaming
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
