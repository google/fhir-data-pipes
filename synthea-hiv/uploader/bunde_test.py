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

import bundle
import test_util


class BundleTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    content = test_util.read_file('bundle')
    self.each_bundle = bundle.Bundle('anything', content)

  def test_extract_resources(self):
    self.each_bundle.extract_resources()
    self.assertEqual(self.each_bundle.openmrs_patient.base.original_id,
                     '495b7301-3b1b-9283-0334-8b574c3f9424')
    self.assertEqual(len(self.each_bundle.openmrs_encounters), 65)
    encounter = self.each_bundle.openmrs_encounters[0]
    self.assertEqual(encounter.base.original_id,
                     '5f670aae-c503-52a5-6c06-4fdd70f11d2b')
    self.assertEqual(len(self.each_bundle.openmrs_observations), 12)
    observation = self.each_bundle.openmrs_observations[0]
    self.assertEqual(observation.base.original_id,
                     '7f714e6d-954e-d209-4586-fe3ae1b2e88a')
