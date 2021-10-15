# Sample Data

This directory contains sample data from running the Synthea
[generator](../generator). Each JSON file is a FHIR Bundle with a patient's
medical history; the Bundle contains FHIR resources such as Patients,
Encounters, and Observations.

In total, there are 80 patients, 4042 Encounters, and 17314 Observations. The
sample data in this folder is 110MB in size.


### Useful Command

To get the count of the number of Encounters and Observations, the command below
was used. 

```bash
RESOURCE_NAME=fhir_resource_name # e.g. "Observation" or "Encounter" 

cat synthea-hiv/sample_data/*.json | \
jq ".entry[].resource.resourceType | select(. ==  \"${RESOURCE_NAME}\")"| \
wc -l
```