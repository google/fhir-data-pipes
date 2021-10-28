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
import requests.adapters

FhirClient = Union['GcpClient', 'OpenMrsClient', 'HapiClient']


def _process_response(response: requests.Response) -> Dict[str, str]:
  if response.status_code >= 400:
    raise ValueError('POST to %s failed with code %s and response:\n %s' %
                     (response.url, response.status_code, response.text))
  return json.loads(response.text)


def _setup_session(base_url: str):
  session = requests.Session()
  retry = requests.adapters.Retry()
  adapter = requests.adapters.HTTPAdapter(max_retries=retry)
  session.mount(base_url, adapter)
  session.headers.update(
      {'Content-Type': 'application/fhir+json;charset=utf-8'})
  return session


class OpenMrsClient:
  """Client to connect to an OpenMRS Server."""

  def __init__(self, base_url: str):
    self.base_url = base_url
    self.session = _setup_session(self.base_url)
    self.session.auth = ('admin', 'Admin123')
    self.response = None

  def post_single_resource(self, resource: str, data: Dict[str, str]):
    url = f'{self.base_url}/{resource}'
    response_ = self.session.post(url, json.dumps(data))
    self.response = _process_response(response_)

  def get_resource(self, resource: str):
    url = f'{self.base_url}/{resource}'
    response_ = self.session.get(url)
    self.response = _process_response(response_)
    return self.response


class GcpClient:
  """Client to connect to GCP FHIR Store."""

  def __init__(self, base_url: str):
    self.base_url = base_url
    self.session = _setup_session(base_url)
    self._auth_req = google.auth.transport.requests.Request()
    self._creds, _ = google.auth.default()
    self.response = None

  def _add_auth_token(self):
    self._creds.refresh(self._auth_req)
    auth_dict = {'Authorization': f'Bearer {self._creds.token}'}
    self.session.headers.update(auth_dict)

  def post_bundle(self, data: Dict[str, str]):
    self._add_auth_token()
    response_ = self.session.post(self.base_url, json.dumps(data))
    self.response = _process_response(response_)

  def post_single_resource(self, resource: str, data: Dict[str, str]):
    self._add_auth_token()
    url = f'{self.base_url}/{resource}'
    response_ = self.session.post(url, json.dumps(data))
    self.response = _process_response(response_)

  def get_resource(self, resource: str):
    self._add_auth_token()
    url = f'{self.base_url}/{resource}'
    response_ = self.session.get(url)
    self.response = _process_response(response_)
    return self.response


class HapiClient(OpenMrsClient):
  """Client to connect to HAPI FHIR Server."""

  def __init__(self, base_url: str):
    super().__init__(base_url)
    self.session.auth = ('hapi', 'hapi')

  def post_bundle(self, data: Dict[str, str]):
    response_ = self.session.post(self.base_url, json.dumps(data))
    self.response = _process_response(response_)
