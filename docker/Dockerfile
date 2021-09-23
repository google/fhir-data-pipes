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

# Build for us-central1-docker.pkg.dev/cloud-build-fhir/openmrs/openmrs-reference-application-distro:latest image

FROM openmrs/openmrs-reference-application-distro:nightly

RUN apt-get clean && apt-get update && apt-get install -y dialog apt-utils

RUN apt-get install -y locales

RUN locale-gen

RUN curl https://addons.openmrs.org/api/v1/addon/org.openmrs.module.atomfeed/1.0.13/download --create-dirs -o /usr/local/tomcat/.OpenMRS/modules/atomfeed-1.0.13.omod

CMD ["/usr/local/tomcat/startup.sh"]