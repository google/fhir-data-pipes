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

import query_lib


class PatientQueryTest(unittest.TestCase):

  def test_single_code_with_values(self):
    patient_query = query_lib.PatientQuery()
    patient_query.include_obs_values_in_time_range(
        'TEST_CODE', ['VAL1', 'VAL2'], '2021-06-01', '2021-07-10')
    sql_constraint = patient_query.all_constraints_sql()
    self.assertEqual(sql_constraint, (
        '((dateTime >= "2021-06-01" AND dateTime <= "2021-07-10" AND '
        'coding.code="TEST_CODE" AND value.string IN ("VAL1","VAL2")))'))

  def test_two_codes_with_values_and_range(self):
    patient_query = query_lib.PatientQuery()
    patient_query.include_obs_values_in_time_range(
        'TEST_CODE1', ['VAL1', 'VAL2'], '2021-06-01', '2021-07-10')
    patient_query.include_obs_in_value_and_time_range(
        'TEST_CODE2', 0.1, None, '2021-07-09', '2021-07-10')
    sql_constraint = patient_query.all_constraints_sql()
    self.assertEqual(sql_constraint, (
        '((dateTime >= "2021-06-01" AND dateTime <= "2021-07-10" AND '
        'coding.code="TEST_CODE1" AND value.string IN ("VAL1","VAL2")) '
        'OR (dateTime >= "2021-07-09" AND dateTime <= "2021-07-10" AND '
        'coding.code="TEST_CODE2" AND  value.quantity.value >= 0.1 ))'))

  def test_two_codes_with_values_and_range_and_other_codes(self):
    patient_query = query_lib.PatientQuery()
    patient_query.include_obs_values_in_time_range(
        'TEST_CODE1', ['VAL1', 'VAL2'], '2021-06-01', '2021-07-10')
    patient_query.include_obs_in_value_and_time_range(
        'TEST_CODE2', 0.1, None, '2021-07-09', '2021-07-10')
    patient_query.include_all_other_codes()
    sql_constraint = patient_query.all_constraints_sql()
    self.assertEqual(sql_constraint, (
        '((dateTime >= "2021-06-01" AND dateTime <= "2021-07-10" AND '
          'coding.code="TEST_CODE1" AND value.string IN ("VAL1","VAL2")) '
        'OR (dateTime >= "2021-07-09" AND dateTime <= "2021-07-10" AND '
            'coding.code="TEST_CODE2" AND  value.quantity.value >= 0.1 ) '
        'OR (coding.code!="TEST_CODE1" AND coding.code!="TEST_CODE2" AND TRUE))'
    ))
