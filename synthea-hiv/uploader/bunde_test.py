import json
import unittest

import bundle


class BundleTest(unittest.TestCase):

  def _read_file(self, resource):
    with open(f'test_files/test_{resource}.json') as f:
      return json.loads(f.read())

  def setUp(self):
    super().setUp()
    content = self._read_file('bundle')
    self.each_bundle = bundle.Bundle('anything', content)

  def test_extract_resources(self):
    self.each_bundle.extract_resources()

    self.assertEqual(self.each_bundle.openmrs_patient.original_id,
                     '2a76c0e3-1e73-2109-3d5e-8a8871fe35d7')

    self.assertEqual(len(self.each_bundle.openmrs_encounters), 1)
    encounter = self.each_bundle.openmrs_encounters[0]
    self.assertEqual(encounter.original_id,
                     'e1846945-43bb-3cfe-1faf-66cf99935c7c')

    self.assertEqual(len(self.each_bundle.openmrs_observations), 1)
    observation = self.each_bundle.openmrs_observations[0]
    self.assertEqual(observation.original_id,
                     'e0f03934-1b25-6f93-e437-b554af10675e')
