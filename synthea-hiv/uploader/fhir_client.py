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
from typing import Dict
import google.auth
import google.auth.transport.requests
import logger_util
import requests


class BaseClient:

  def __init__(self, base_url: str):
    self._base_url = base_url
    self._headers = {'Content-Type': 'application/fhir+json;charset=utf-8'}
    self.response = None
    self.logger = logger_util.create_logger(self.__class__.__module__,
                                            self.__class__.__name__)

  def post_data(self, **kwargs):
    raise NotImplementedError

  def _process_response(self, response: requests.Response):
    if response.status_code >= 400:
      self.logger.debug(response.request.body)
      raise ValueError('POST to %s failed with code %s and response:\n %s' %
                       (response.url, response.status_code, response.text))
    self.response = json.loads(response.text)


class OpenMrsClient(BaseClient):
  """Client to connect to an OpenMRS Server."""

  def __init__(self, base_url: str):
    super().__init__(base_url)
    self._auth = ('admin', 'Admin123')

  def post_data(self, resource: str, data: Dict[str, str]):
    url = f'{self._base_url}/{resource}'
    self._process_response(
        requests.post(
            url=url,
            data=json.dumps(data),
            auth=self._auth,
            headers=self._headers))


class GcpClient(BaseClient):
  """Client to connect to GCP FHIR Store."""

  def __init__(self, base_url: str):
    super().__init__(base_url)
    self._auth_req = google.auth.transport.requests.Request()
    self._creds, _ = google.auth.default()
    self._auth = None

  def post_data(self, data: Dict[str, str]):
    self._creds.refresh(self._auth_req)
    self._headers['Authorization'] = f'Bearer {self._creds.token}'
    self._process_response(
        requests.post(
            url=self._base_url, data=json.dumps(data), headers=self._headers))
