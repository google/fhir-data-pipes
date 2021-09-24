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

import uploader


class UploaderTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_client = mock.MagicMock()
    self.mock_bundle = mock.MagicMock()
    self._upload_resource = mock.patch.object(
        uploader.Uploader, '_upload_resource', return_value='123').start()

  def test_upload_bundle(self):
    self.mock_bundle.openmrs_patient = mock.MagicMock()
    upload_handler = uploader.Uploader(self.mock_client)
    upload_handler.upload_openmrs_bundle(self.mock_bundle)
    self.assertTrue(self._upload_resource.called)
    self.assertEqual(self.mock_bundle.openmrs_patient.base.new_id, '123')

  def test_upload_bundle_gcp(self):
    self.mock_bundle.patient = None
    upload_handler = uploader.Uploader(self.mock_client)
    upload_handler.upload_bundle(self.mock_bundle)
    self.assertFalse(self._upload_resource.called)
