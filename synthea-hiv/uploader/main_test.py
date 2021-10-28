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
import tempfile
import unittest

import main
import mock


class MainTest(unittest.TestCase):

  def test_list_all_files(self):

    with tempfile.TemporaryDirectory() as tmpdirname:

      bundle_dic = main.list_all_files(tmpdirname)
      self.assertEqual(bundle_dic, {
          'practitioner': set(),
          'hospital': set(),
          'patient_history': set()
      })

      with tempfile.NamedTemporaryFile(suffix='.json', dir=tmpdirname):
        bundle_dic = main.list_all_files(tmpdirname)
      self.assertEqual(len(bundle_dic['patient_history']), 1)

  def test_convert_to_bundle(self):
    config = {'resource': 'patient', 'name': {'first': 'Super', 'last': 'man'}}
    temp_file = tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8')
    json.dump(config, temp_file)
    temp_file.flush()
    bundle = main.convert_to_bundle(temp_file.name)
    self.assertEqual(bundle.file_name, temp_file.name)

  def test_fetch_location(self):
    mock_sink = mock.MagicMock()
    mock_sink.get_resource.return_value = {
        'entry': [{
            'not_location': 'location'
        }]
    }
    location = main.fetch_location(mock_sink)
    self.assertEqual(
        location, {'8d6c993e-c2cc-11de-8d13-0010c6dffd0f': 'Unknown Location'})

    mock_sink.get_resource.return_value = {
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
    location = main.fetch_location(mock_sink)
    self.assertEqual(
        location, {
            '8d6c993e-c2cc-11de-8d13-0010c6dffd0f': 'Unknown Location',
            '7f65d926-57d6-4402-ae10-a5b3bcbf7986': 'Pharmacy',
            '7fdfa2cb-bc95-405a-88c6-32b7673c0453': 'Laboratory'
        })
