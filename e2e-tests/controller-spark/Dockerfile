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

##############################################################################################
# NOTE: This Docker image is designed to run exclusively in Cloud Build environment.
# Where the entire repository is automatically mounted at /workspace.
# Scripts (controller_spark_sql_validation.sh, lib/parquet_utils.sh)
# and JARs (parquet-tools-1.11.1.jar) are accessed directly from the mounted repository
# at runtime so we do **NOT** COPY them into the image.
#
# If you ever need to run this image **outside** Cloud Build, you must bind-mount the repo
# to /workspace or manually copy the required files into the container.
##############################################################################################

USER root
RUN apt-get update && apt-get install -y jq curl python3 python3-pip
RUN pip3 install virtualenv google-auth requests

ENV PARQUET_SUBDIR="dwh"
ENV DOCKER_NETWORK="--use_docker_network"
ENV HOME_DIR="/workspace/e2e-tests/controller-spark"
ENV DWH_TYPE="PARQUET"

ENTRYPOINT cd ${HOME_DIR}; ./controller_spark_sql_validation.sh ${HOME_DIR} ${PARQUET_SUBDIR} ${DOCKER_NETWORK} ${DWH_TYPE}
