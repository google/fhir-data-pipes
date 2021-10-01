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

# Dockerfile used to create fhir-analytics/synthea-generator

FROM openjdk:16-jdk-alpine
RUN apk --no-cache add openssl wget git curl

WORKDIR /synthea-hiv
RUN wget https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
COPY hiv_simple.json hiv_simple.json
COPY hiv_simple hiv_simple

ENV POPULATION 100

CMD java -jar synthea-with-dependencies.jar \
    -p ${POPULATION} \
    --exporter.years_of_history 0 \
    --generate.only_alive_patients true \
    --generate.max_attempts_to_keep_patient 100 \
    -s 1632186774891 \
    -d ./ \
    -m hiv_simple \
    -k hiv_simple.json
