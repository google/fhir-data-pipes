
FROM openjdk:8

ARG WORK_DIR="/usr/src/Main"
COPY target/fhir-batch-etl-bundled-0.1.0-SNAPSHOT.jar ${WORK_DIR}/app.jar
WORKDIR ${WORK_DIR}

ENV GCP_FHIR_STORE="projects/PROJECT/locations/LOCATION/datasets/DATASET/"
ENV SERVER_URL="http://localhost:8099/openmrs"
ENV JSESSION_ID="9647DF11FE862BADEC04D59F5B25E915"
ENV SEARCH_LIST="Patient,Encounter,Observation"
ENV BATCH_SIZE=10
ENV TARGET_PARALLELISM=10


RUN echo 'Batch Job started'
VOLUME [ "${WORK_DIR}" ]

ENTRYPOINT java -cp app.jar org.openmrs.analytics.FhirEtl --serverUrl=${SERVER_URL} --jsessionId=${JSESSION_ID} \
           --searchList=${SEARCH_LIST} --batchSize=${BATCH_SIZE} --targetParallelism=${TARGET_PARALLELISM} --gcpFhirStore=${GCP_FHIR_STORE}