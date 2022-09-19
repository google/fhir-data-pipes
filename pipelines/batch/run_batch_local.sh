#!/usr/bin/env bash

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


# Script to run batch pipeline locally
# Make sure to run this script with the current path in the terminal being the
# same as the directory this scrip is in, so all you have to do is:
#     ./run_batch_local.sh
#
# To override any of the ENV vars defined in the Dockerfile run the script and
# add: -e ENV_VAR_NAME=VALUE for each variable you want to change
# For example:
# ./run_batch_local.sh -e OPENMRS_USERNAME="admin" -e JDBC_MODE_ENABLED=true


ENV_VARS=$@
echo "The env variables passed in to override are: ${ENV_VARS}"

echo "Building the Dockerfile. The image tag will be: batch-pipeline:local"
docker build -t batch-pipeline:local .

cd ../..
echo "Running the pipeline using the Docker image built"
docker run -it -v "$(pwd)":/workspace --network=cloudbuild  \
  ${ENV_VARS} \
  batch-pipeline:local
