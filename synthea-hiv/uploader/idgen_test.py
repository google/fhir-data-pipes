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
import idgen


class IdGenTest(unittest.TestCase):

  def test_luhn_id_generator(self):
    self.assertEqual('98056J', idgen.luhn_id_generator(98056))
    self.assertEqual('98056J', idgen.luhn_id_generator('98056'))

  def test_convert_to_int(self):
    self.assertEqual('89', idgen.convert_to_int('hi'))
    self.assertEqual('', idgen.convert_to_int(''))
    with self.assertRaises(AttributeError):
      idgen.convert_to_int(123)
