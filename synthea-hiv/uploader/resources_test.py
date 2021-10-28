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

import resources
import test_util


class ResourcesTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.patient_data = test_util.read_file('patient')
    self.encounter_data = test_util.read_file('encounter')
    self.observation_data = test_util.read_file('observation')

  def test_openmrs_convert_patient(self):
    test_patient = resources.Patient(self.patient_data)
    self.assertEqual(test_patient.base.original_id,
                     '2a76c0e3-1e73-2109-3d5e-8a8871fe35d7')
    self.assertEqual(test_patient.base.json['identifier'], [])
    test_patient.openmrs_convert()
    self.assertEqual(test_patient.base.json['identifier'], [{
        'extension': [{
            'url': 'http://fhir.openmrs.org/ext/patient/identifier#location',
            'valueReference': {
                'reference': 'Location/8d6c993e-c2cc-11de-8d13-0010c6dffd0f',
                'type': 'Location',
                'display': 'Unknown Location'
            }
        }],
        'use': 'official',
        'type': {
            'text': 'OpenMRS ID'
        },
        'value': '21763053157321093455818871653547C',
        'id': test_patient.base.original_id
    }])

  def test_openmrs_convert_encounter(self):
    test_encounter = resources.Encounter(self.encounter_data)
    self.assertEqual(test_encounter.base.original_id,
                     'e1846945-43bb-3cfe-1faf-66cf99935c7c')
    location = ('456', 'World')
    test_encounter.openmrs_convert('123', location)
    self.assertEqual(test_encounter.base.json['type'], [{
        'coding': [{
            'system': 'http://fhir.openmrs.org/code-system/encounter-type',
            'code': '5021b1a1-e7f6-44b4-ba02-da2f2bcf8718',
            'display': 'Attachment Upload'
        }]
    }])

    self.assertEqual(test_encounter.base.json['location'], [{
        'location': {
            'reference': 'Location/456',
            'display': 'World'
        }
    }])
    self.assertEqual(test_encounter.base.json['subject']['reference'],
                     'Patient/123')

    with self.assertRaises(KeyError):
      _ = test_encounter.base.json['identifier']
      _ = test_encounter.base.json['participant']

  def test_openmrs_convert_observation(self):
    test_observation = resources.Observation(self.observation_data)
    self.assertEqual(test_observation.base.original_id,
                     'e0f03934-1b25-6f93-e437-b554af10675e')

    encounter = resources.Encounter(self.encounter_data)
    encounter_list = [encounter]
    with self.assertRaises(TypeError):
      test_observation.openmrs_convert('123', encounter_list)

    encounter.base.new_id = '5678'
    test_observation.openmrs_convert('123', encounter_list)

    self.assertEqual(test_observation.base.json['subject']['reference'],
                     'Patient/123')
    self.assertEqual(test_observation.base.json['encounter']['reference'],
                     'Encounter/5678')

    with self.assertRaises(KeyError):
      _ = test_observation.base.json['code']['coding'][0]['system']
      _ = test_observation.base.json['valueCodeableConcept']['coding'][0][
          'system']
