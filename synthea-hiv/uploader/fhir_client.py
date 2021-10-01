# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
"""Connections to a FHIR Server."""

import json
from typing import Dict, Union
import google.auth
import google.auth.transport.requests
import requests

FhirClient = Union['GcpClient', 'OpenMrsClient']


def process_response(response: requests.Response) -> Dict[str, str]:
  if response.status_code >= 400:
    raise ValueError('POST to %s failed with code %s and response:\n %s' %
                     (response.url, response.status_code, response.text))
  return json.loads(response.text)


class OpenMrsClient:
  """Client to connect to an OpenMRS Server."""

  def __init__(self, base_url: str):
    self._base_url = base_url
    self._headers = {'Content-Type': 'application/fhir+json;charset=utf-8'}
    self._auth = ('admin', 'Admin123')
    self.response = None

  def post_single_resource(self, resource: str, data: Dict[str, str]):
    url = f'{self._base_url}/{resource}'
    self.response = process_response(
        requests.post(
            url=url,
            data=json.dumps(data),
            auth=self._auth,
            headers=self._headers))


class GcpClient:
  """Client to connect to GCP FHIR Store."""

  def __init__(self, base_url: str):
    self._base_url = base_url
    self._headers = {'Content-Type': 'application/fhir+json;charset=utf-8'}
    self._auth_req = google.auth.transport.requests.Request()
    self._creds, _ = google.auth.default()
    self.response = None

  def post_bundle(self, data: Dict[str, str]):
    self._creds.refresh(self._auth_req)
    self._headers['Authorization'] = f'Bearer {self._creds.token}'
    self.response = process_response(
        requests.post(
            url=self._base_url, data=json.dumps(data), headers=self._headers))

  def post_single_resource(self, resource: str, data: Dict[str, str]):
    self._creds.refresh(self._auth_req)
    self._headers['Authorization'] = f'Bearer {self._creds.token}'
    url = f'{self._base_url}/{resource}'
    self.response = process_response(
        requests.post(url=url, data=json.dumps(data), headers=self._headers))
