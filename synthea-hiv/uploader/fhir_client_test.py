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

import unittest
from unittest import mock
import fhir_client
import logger_util

def mock_requests_post(**kwargs):

  class MockResponse:

    def __init__(self, text, status_code, url):
      self.text = text
      self.status_code = status_code
      self.url = url

      if status_code >= 400:
        self.request = mock.MagicMock()
        self.request.body  = "this happened"

  if 'localhost/Patient' in kwargs['url']:
    return MockResponse('{"resourceType":"Local"}', 200, kwargs['url'])
  elif 'google' in kwargs['url']:
    return MockResponse('{"resourceType":"Google"}', 201, kwargs['url'])

  return MockResponse(None, 404, kwargs['url'])


class FhirClientTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    mock.patch(
        'google.auth.transport.requests.Request',
        return_value=mock.MagicMock()).start()
    mock.patch(
        'google.auth.default', return_value=(mock.MagicMock(), '_')).start()
    mock.patch('requests.post', side_effect=mock_requests_post).start()

  def test_post_data_local(self):
    client = fhir_client.OpenMrsClient('http://localhost')
    client.post_data(resource='Patient', data={'hi': 'bob'})
    self.assertEqual(client.response, {'resourceType': 'Local'})

  def test_post_data_gcp(self):
    client = fhir_client.GcpClient('http://googleapis')
    client.post_data(data={'hi': 'bob'})
    self.assertEqual(client.response, {'resourceType': 'Google'})

  @mock.patch.object(logger_util, 'create_logger')
  def test_post_data_error(self, _):
    client = fhir_client.OpenMrsClient('http://random')
    with self.assertRaises(ValueError):
      client.post_data(resource='Patient', data={'hi': 'mom'})
    client.logger.debug.assert_called_with('this happened')
