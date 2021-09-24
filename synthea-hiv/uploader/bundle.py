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
"""Bundle to upload to FHIR Server.

If uploading to OpenMRS, we need to extract the Patient, all the Encounter and
all the Observations from the JSON because OpenMRS does not support uploading
Bundle resources directly.
"""

import pathlib
from typing import Dict

import logger_util
import resources


class Bundle:
  """Object to be uploaded to FHIR server."""

  def __init__(self, file_name: pathlib.PosixPath, bundle_dict: Dict[str, str]):
    self.bundle_dict = bundle_dict
    self.file_name = file_name
    self._openmrs_patient = None
    self._openmrs_encounters = []
    self._openmrs_observations = []
    self.logger = logger_util.create_logger(self.__class__.__module__,
                                            self.__class__.__name__)

  @property
  def openmrs_patient(self):
    return self._openmrs_patient

  @property
  def openmrs_encounters(self):
    return self._openmrs_encounters

  @property
  def openmrs_observations(self):
    return self._openmrs_observations

  @openmrs_patient.setter
  def openmrs_patient(self, patient: resources.Patient):
    self._openmrs_patient = patient

  def add_encounter(self, encounter: resources.Encounter):
    self.openmrs_encounters.append(encounter)

  def add_observation(self, observation: resources.Observation):
    self.openmrs_observations.append(observation)

  def extract_resources(self):
    """Used to extract resources for OpenMRS uploads.

    Loops through the JSON file, extracting and setting the resources needed

    to upload to OpenMRS.
    """

    self.logger.debug('Splitting bundle from  %s' % self.file_name)
    for entry in self.bundle_dict['entry']:

      if entry['resource']['resourceType'] == 'Patient':
        self.openmrs_patient = resources.Patient(entry['resource'])

      if entry['resource']['resourceType'] == 'Encounter':
        self.add_encounter(resources.Encounter(entry['resource']))

      if entry['resource']['resourceType'] == 'Observation':
        self.add_observation(resources.Observation(entry['resource']))

  def save_mapping(self):
    # TODO(omarismail): implement.
    pass

  def __repr__(self):
    if self.patient:
      return str({
          'new_patient_id': self.openmrs_patient.base.new_id,
          'encounters': self.openmrs_encounters.base,
          'observations': self.openmrs_observations.base
      })
    else:
      return str(self.bundle_dict)

  def __str__(self):
    return self.__repr__()
