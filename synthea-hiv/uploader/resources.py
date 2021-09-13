"""Resources that OpenMRS can upload. This class is only used for OpenMRS."""

import random
from typing import Dict, List
import uuid

import idgen


class BaseResource:
  """Base resource. Has a JSON, original_id, and potentially, a new_id."""

  def __init__(self, json: Dict[str, str]):
    self.json = json
    self.original_id = self.json['id']
    self.new_id = None

  def openmrs_convert(self, *args):
    raise NotImplementedError

  def inject_id(self):
    """OpenMRS requires each key to have an id field with a random UUID.

    This function injects random UUID.
    """
    for _, item in self.json.items():
      if isinstance(item, list):
        for element in item:
          if 'id' not in element:
            element['id'] = str(uuid.uuid4())

  def extract_id(self, key: str) -> str:
    """Helper function to get original ids refereneced.

    Some resources contain reference to other resources. For example, the
    Encounter resource references the ID of the Patient. This function extracts
    this ID.

    Args:
      key: resource which is being referenced

    Returns:
      ID of resource
    """
    return self.json[key]['reference'].split(':')[-1]

  def __repr__(self):
    return str({'original_id': self.original_id, 'new_id': self.new_id})

  def __str__(self):
    return self.__repr__()


class Patient(BaseResource):
  """Patient resource to upload to OpenMRS."""

  def openmrs_convert(self):
    """Add fields to make Patient uploadable to OpenMRS."""

    openmrs_id = idgen.luhn_id_generator(
        idgen.convert_to_int(self.original_id))    

    self.json['identifier'].append({
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
        'id': self.original_id
    })

    self.inject_id()


class Encounter(BaseResource):
  """Encounter resource to upload to OpenMRS."""

  def openmrs_convert(self, new_patient_id: str):
    """Change fields to make Encounter uploadable to OpenMRS.

    Args:
      new_patient_id: new ID of the Patient after the Patient resource has been
        uploaded to OpenMRS.
    """

    self.json['subject']['reference'] = 'Patient/' + new_patient_id
    self.json['type'] = [{
        'coding': [{
            'system': 'http://fhir.openmrs.org/code-system/encounter-type',
            'code': '5021b1a1-e7f6-44b4-ba02-da2f2bcf8718',
            'display': 'Attachment Upload'
        }]
    }]

    self.json.pop('identifier', None)
    self.json.pop('participant', None)


class Observation(BaseResource):
  """Observation resource to upload to OpenMRS."""

  def openmrs_convert(self, new_patient_id: str,
                      encounter_list: List[Encounter]):
    """Change fields to make Encounter uploadable to OpenMRS.

    Args:
      new_patient_id: new ID of the Patient after the Patient resource has been
        uploaded to OpenMRS.
      encounter_list:  list of encounters to update the encounter reference id
    """
    self.json['subject']['reference'] = 'Patient/' + new_patient_id

    for encounter in encounter_list:
      if encounter.original_id == self.extract_id('encounter'):
        self.json['encounter']['reference'] = 'Encounter/' + encounter.new_id
        break

    self.json['code']['coding'][0].pop('system')
    if 'valueCodeableConcept' in self.json:
      self.json['valueCodeableConcept']['coding'][0].pop('system')
    if 'valueQuantity' in self.json:
      self.json['valueQuantity']['value'] = int(
          self.json['valueQuantity']['value'])
