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

FROM adoptopenjdk/maven-openjdk11

RUN apt-get update && apt-get install -y jq  python3.7 python3-pip 
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1
RUN pip3 install virtualenv google-auth requests

COPY pipeline_validation.sh pipeline_validation.sh
COPY parquet-tools-1.11.1.jar  parquet-tools-1.11.1.jar
ENV PARQUET_SUBDIR="NON_JDBC"
ENV DOCKER_NETWORK="--use_docker_network"
ENV STREAMING_TEST=""
ENV HAPI_TEST=""
ENV HOME_DIR="/workspace/e2e-tests"

ENTRYPOINT cd ${HOME_DIR}; ./pipeline_validation.sh ${HOME_DIR} ${PARQUET_SUBDIR} ${DOCKER_NETWORK} ${STREAMING_TEST} ${HAPI_TEST}
