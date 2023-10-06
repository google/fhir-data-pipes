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

# This creates a docker image for running the controller web-app. It is expected
# that in real use-cases, the config dir will be mounted to the host machine.

# We need to use the 3.10 version because google-fhir-views requirest <3.11.
FROM jupyter/scipy-notebook:python-3.10

COPY --chown=jovyan:users queries_and_views.ipynb .
COPY --chown=jovyan:users requirements.txt .

RUN pip install -r requirements.txt

# TODO add this to the continuous integration.
