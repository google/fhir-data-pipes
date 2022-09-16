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

import tempfile
import unittest

import main

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
