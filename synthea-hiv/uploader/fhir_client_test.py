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


def mock_requests_post(url, _):

  class MockResponse:

    def __init__(self, text, status_code, url):
      self.text = text
      self.status_code = status_code
      self.url = url

  if 'localhost/Patient' in url:
    return MockResponse('{"resourceType":"Local"}', 200, url)
  elif 'google' in url:
    return MockResponse('{"resourceType":"Google"}', 201, url)
  return MockResponse(None, 404, url)


class FhirClientTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    mock_session = mock.MagicMock()
    mock_session.post.side_effect = mock_requests_post
    mock.patch('requests.Session', return_value=mock_session).start()
    mock.patch(
        'google.auth.transport.requests.Request',
        return_value=mock.MagicMock()).start()
    mock.patch(
        'google.auth.default', return_value=(mock.MagicMock(), '_')).start()

  def test_post_data_local(self):
    client = fhir_client.OpenMrsClient('http://localhost')
    client.post_single_resource(resource='Patient', data={'hi': 'bob'})
    self.assertEqual(client.response, {'resourceType': 'Local'})

  def test_post_bundle_gcp(self):
    client = fhir_client.GcpClient('http://googleapis')
    client.post_bundle(data={'hi': 'bob'})
    self.assertEqual(client.response, {'resourceType': 'Google'})

  def test_post_resource_error(self):
    client = fhir_client.HapiClient('http://random')
    with self.assertRaises(ValueError):
      client.post_single_resource(resource='Patient', data={'hi': 'mom'})
