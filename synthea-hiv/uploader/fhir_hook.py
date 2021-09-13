"""Hooks to connect to a FHIR Server."""

import json
from typing import Tuple

import google.auth
import google.auth.transport.requests
import log_mixin
import requests

class BaseHook(log_mixin.LoggingMixin):
  def __init__(self,
               base_url: str,
               auth: Tuple[str, str] = ('admin', 'Admin123')):
    self.base_url = base_url
    self.headers = {'Content-Type': 'application/fhir+json;charset=utf-8'}
    self.auth = auth
    self.response = None
  
  def post_data(self, **kwargs):
    raise NotImplementedError

class OpenMRSHook(BaseHook):
  """Hook to connect to an OpenMRS Server."""

  def post_data(self, **kwargs):
    resource = kwargs.pop('resource', '')
    url = f'{self.base_url}/{resource}'
    data = kwargs.pop('data')
    response = requests.post(
        url=url, data=json.dumps(data), auth=self.auth, headers=self.headers)
    if response.status_code >= 400:
      self.logger.debug(response.request.body)
      self.logger.error(response.text)
      raise ValueError
    self.response = json.loads(response.text)


class GcpHook(BaseHook):
  """Hook to GCP FHIR Store. Injects GCP Credentials to base class."""

  def __init__(self, base_url: str):
    super().__init__(base_url)
    self.auth_req = google.auth.transport.requests.Request()
    self.creds, _ = google.auth.default()
    self.auth = None

  def post_data(self, **kwargs):
    self.creds.refresh(self.auth_req)
    self.headers['Authorization'] = f'Bearer {self.creds.token}'
    resource = kwargs.pop('resource', '')
    url = f'{self.base_url}/{resource}'
    data = kwargs.pop('data')
    response = requests.post(
        url=url, data=json.dumps(data), auth=self.auth, headers=self.headers)
    if response.status_code >= 400:
      self.logger.debug(response.request.body)
      self.logger.error(response.text)
      raise ValueError
    self.response = json.loads(response.text)