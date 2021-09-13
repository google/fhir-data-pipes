"""Bundle to upload to FHIR Server.

If uploading to OpenMRS, we need to extract the Patient, all the Encounter and
all the Observations from the json.
"""

import pathlib
from typing import Dict

import log_mixin
import resources


class Bundle(log_mixin.LoggingMixin):
  """Object to be uploaded to FHIR server."""

  def __init__(self, file_name: pathlib.PosixPath, json: Dict[str, str]):
    self.json = json
    self.file_name = file_name
    self._openmrs_patient = None
    self._openmrs_encounters = []
    self._openmrs_observations = []

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
    """Loops through the JSON file, extracting and setting the resources needed to upload to OpenMRS."""

    self.logger.debug('Splitting bundle from  %s' % self.file_name)
    for entry in self.json['entry']:

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
          'new_patient_id': self.openmrs_patient.new_id,
          'encounters': self.openmrs_encounters,
          'observations': self.openmrs_observations
      })
    else:
      return str(self.json)

  def __str__(self):
    return self.__repr__()
