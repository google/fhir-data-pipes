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

import json
import pathlib
import tempfile
import unittest
from unittest import mock

import uploader


class UploaderTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_client = mock.MagicMock()
    self._upload_resource = mock.patch.object(
        uploader.Uploader, '_upload_resource', return_value='123').start()

  def test_upload_bundle_openmrs(self):
    mock_location = mock.MagicMock()
    upload_handler = uploader.Uploader(self.mock_client)
    # We need to keep the file until the end of the test scope, hence next var.
    temp_file = self._create_file()
    upload_handler.upload_openmrs_bundle(pathlib.PosixPath(temp_file.name),
                                         mock_location)
    self.assertTrue(self._upload_resource.called)

  def test_upload_bundle_gcp(self):
    upload_handler = uploader.Uploader(self.mock_client)
    temp_file = self._create_file()
    upload_handler.upload_bundle(pathlib.PosixPath(temp_file.name))
    self.assertFalse(self._upload_resource.called)

  def test_fetch_location(self):
    upload_handler = uploader.Uploader(self.mock_client)
    self.mock_client.get_resource.return_value = {
        'entry': [{
            'not_location': 'location'
        }]
    }
    location = upload_handler.fetch_location()
    self.assertEqual(
        location, {'8d6c993e-c2cc-11de-8d13-0010c6dffd0f': 'Unknown Location'})

    self.mock_client.get_resource.return_value = {
        'entry': [{
            'resource': {
                'resourceType': 'Location',
                'id': '8d6c993e-c2cc-11de-8d13-0010c6dffd0f',
                'text': {
                    'status': 'generated',
                    'div':
                        '<div xmlns="http://www.w3.org/1999/xhtml"><h2>Unknown '
                        'Location</h2><h2/></div>'
                },
                'status': 'active',
                'name': 'Unknown Location'
            }
        }, {
            'resource': {
                'resourceType': 'Location',
                'id': '7f65d926-57d6-4402-ae10-a5b3bcbf7986',
                'text': {
                    'status': 'generated',
                    'div':
                        '<div '
                        'xmlns="http://www.w3.org/1999/xhtml"><h2>Pharmacy</h2></div>'
                },
                'status': 'active',
                'name': 'Pharmacy'
            }
        }, {
            'resource': {
                'resourceType': 'Location',
                'id': '7fdfa2cb-bc95-405a-88c6-32b7673c0453',
                'text': {
                    'status': 'generated',
                    'div':
                        '<div '
                        'xmlns="http://www.w3.org/1999/xhtml"><h2>Laboratory</h2></div>'
                },
                'status': 'active',
                'name': 'Laboratory'
            }
        }]
    }
    location = upload_handler.fetch_location()
    self.assertEqual(
        location, {
            '8d6c993e-c2cc-11de-8d13-0010c6dffd0f': 'Unknown Location',
            '7f65d926-57d6-4402-ae10-a5b3bcbf7986': 'Pharmacy',
            '7fdfa2cb-bc95-405a-88c6-32b7673c0453': 'Laboratory'
        })

  def _create_file(self) -> tempfile.NamedTemporaryFile:
    # A Bundle with a single Patient is good enough for most of the tests.
    config = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [{
            "fullUrl": "urn:uuid:495b7301-3b1b-9283-0334-8b574c3f9424",
            "resource": {
                "resourceType": "Patient",
                "id": "495b7301-3b1b-9283-0334-8b574c3f9424",
                "identifier": [{
                    "system": "https://github.com/synthetichealth/synthea",
                    "value": "495b7301-3b1b-9283-0334-8b574c3f9424"
                }],
                "name": [{
                    "use": "official",
                    "family": "Hagenes547",
                    "given": [ "Andreas188" ],
                    "prefix": [ "Mr." ]
                }],
                "telecom": [ {
                    "system": "phone",
                    "value": "555-338-1943",
                    "use": "home"
                }],
                "gender": "male",
                "birthDate": "1950-04-13",
                "multipleBirthInteger": 3
            },
            "request": {
                "method": "POST",
                "url": "Patient"
            }
        }
      ]
    }
    temp_file = tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8')
    json.dump(config, temp_file)
    temp_file.flush()
    return temp_file

  def test_convert_to_bundle(self):
    temp_file = self._create_file()
    bundle = uploader._convert_to_bundle(pathlib.PosixPath(temp_file.name))
    self.assertEqual(bundle.file_name.as_posix(), temp_file.name)
