FROM python:3.7-slim

WORKDIR /workspace
COPY  ./ ./

RUN pip install -r requirements.txt
ENV INPUT_DIR="/workspace/test_files"
ENV CONVERT=""
ENV FHIR_ENDPOINT="http://localhost:8099/openmrs/ws/fhir2/R4"

ENTRYPOINT python main.py ${FHIR_ENDPOINT} --input_dir ${INPUT_DIR} ${CONVERT}