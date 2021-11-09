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
"""Resources that OpenMRS can upload. This class is only used for OpenMRS."""

from typing import Dict, List, Tuple
import uuid

import idgen


class BaseResource:
  """Base resource. Has a JSON, original_id, and potentially, a new_id."""

  def __init__(self, json: Dict[str, str]):
    self.json = json
    self.original_id = self.json['id']
    self.new_id = None

  def __repr__(self):
    return str({'original_id': self.original_id, 'new_id': self.new_id})

  def __str__(self):
    return self.__repr__()


class Patient:
  """Patient resource to upload to OpenMRS."""

  def __init__(self, json: Dict[str, str]):
    self.base = BaseResource(json)

  def openmrs_convert(self):
    """Add fields to make Patient uploadable to OpenMRS."""

    openmrs_id = idgen.luhn_id_generator(
        idgen.convert_to_int(self.base.original_id))

    self.base.json['identifier'].append({
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
        'value': openmrs_id,
        'id': self.base.original_id
    })

    self._inject_id()

  def _inject_id(self):
    """OpenMRS requires each key to have an id field with a random UUID.

    This function injects random UUID.
    """
    for _, item in self.base.json.items():
      if isinstance(item, list):
        for element in item:
          if 'id' not in element:
            element['id'] = str(uuid.uuid4())


class Encounter:
  """Encounter resource to upload to OpenMRS."""

  def __init__(self, json: Dict[str, str]):
    self.base = BaseResource(json)

  def openmrs_convert(self, new_patient_id: str, location: Tuple[str, str]):
    """Change fields to make Encounter uploadable to OpenMRS.

    Args:
      new_patient_id: new ID of the Patient after the Patient resource has been
        uploaded to OpenMRS.
      location: tuple of the form: (location_id, location_name)
    """

    self.base.json['subject']['reference'] = 'Patient/' + new_patient_id
    self.base.json['type'] = [{
        'coding': [{
            'system': 'http://fhir.openmrs.org/code-system/encounter-type',
            'code': '5021b1a1-e7f6-44b4-ba02-da2f2bcf8718',
            'display': 'Attachment Upload'
        }]
    }]

    self.base.json['location'] = [{
        'location': {
            'reference': f'Location/{location[0]}',
            'display': location[1]
        }
    }]
    self.base.json.pop('identifier', None)
    self.base.json.pop('participant', None)
    self.base.json.pop('serviceProvider', None)


class Observation:
  """Observation resource to upload to OpenMRS."""

  def __init__(self, json: Dict[str, str]):
    self.base = BaseResource(json)

  def openmrs_convert(self, new_patient_id: str,
                      encounter_list: List[Encounter]):
    """Change fields to make Encounter uploadable to OpenMRS.

    Args:
      new_patient_id: new ID of the Patient after the Patient resource has been
        uploaded to OpenMRS.
      encounter_list:  list of encounters to update the encounter reference id
    """
    self.base.json['subject']['reference'] = 'Patient/' + new_patient_id
    base_id = self.base.json['encounter']['reference'].split(':')[-1]
    for encounter in encounter_list:
      if encounter.base.original_id == base_id:
        self.base.json['encounter'][
            'reference'] = 'Encounter/' + encounter.base.new_id
        break

    self.base.json['code']['coding'][0].pop('system')
    if 'valueCodeableConcept' in self.base.json:
      self.base.json['valueCodeableConcept']['coding'][0].pop('system')
    if 'valueQuantity' in self.base.json:
      self.base.json['valueQuantity']['value'] = int(
          self.base.json['valueQuantity']['value'])
    if self.base.json['code']['coding'][0][
        'code'] == '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA':
      self.base.json['valueDateTime'] = self.base.json['effectiveDateTime']
