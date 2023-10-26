# Copyright 2022 Google LLC
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

FROM maven:3.8.7-eclipse-temurin-17-focal as build

RUN apt-get update && apt-get install -y nodejs npm
RUN npm cache clean -f && npm install -g n && n stable

WORKDIR /app

COPY ./bunsen ./bunsen
COPY ./pipelines ./pipelines
COPY ./pom.xml ./pom.xml
COPY ./utils ./utils

# Updating license will fail in e2e and there is no point doing it here anyways.
# Note this build can be faster by excluding some uber-jars we don't copy.
RUN mvn --batch-mode clean package -Dlicense.skip=true

FROM eclipse-temurin:17-jdk-focal as main

RUN apt-get update && apt-get install -y libjemalloc-dev

WORKDIR /app

COPY --from=build /app/pipelines/controller/target/controller-bundled.jar .

COPY ./docker/config ./config

# Flink will read the flink-conf.yaml file from this directory.
ENV FLINK_CONF_DIR=/app/config

COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
