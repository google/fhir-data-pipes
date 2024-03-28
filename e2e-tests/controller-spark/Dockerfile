# Copyright 2023 Google LLC
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

FROM bitnami/spark:3.3

USER root
RUN apt-get update && apt-get install -y jq curl python3 python3-pip
RUN pip3 install virtualenv google-auth requests

COPY controller_spark_sql_validation.sh controller_spark_sql_validation.sh
COPY parquet-tools-1.11.1.jar  parquet-tools-1.11.1.jar
ENV PARQUET_SUBDIR="dwh"
ENV DOCKER_NETWORK="--use_docker_network"
ENV HOME_DIR="/workspace/e2e-tests/controller-spark"

ENTRYPOINT cd ${HOME_DIR}; ./controller_spark_sql_validation.sh ${HOME_DIR} ${PARQUET_SUBDIR} ${DOCKER_NETWORK}
