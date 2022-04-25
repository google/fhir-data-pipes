# Copyright 2021 Google LLC
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

FROM python:3.7-slim

WORKDIR /uploader
COPY  ./ ./

RUN pip install -r requirements.txt
ENV INPUT_DIR="./test_files"
ENV CORES=""
ENV CONVERT=""
ENV SINK_TYPE="HAPI"
ENV FHIR_ENDPOINT="http://localhost:8098/fhir"

CMD cd /uploader; python main.py ${SINK_TYPE} \
    ${FHIR_ENDPOINT} --input_dir ${INPUT_DIR} ${CORES} ${CONVERT}
