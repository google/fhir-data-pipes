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
import query_lib as ql

# NOTE: For these test to run the GOOGLE_APPLICATION_CREDENTIALS have to be st
# and the credentials should have access to below dataset
# TODO(gdevanla): Update this to integration database
_BIGQUERY_DATASET = 'learnbq-345320.fhir_sample'


class BiqQueryPatientQueryTest(unittest.TestCase):

  def test_basic_query(self):

    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df)

  def test_basic_query_with_system(self):

    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    pq.encounter_constraints(type_system='system1001')
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df)

  def test_basic_query_with_codes(self):
    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    pq.encounter_constraints(type_codes=['code1000', 'code3000'])
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df)

  def test_basic_query_with_location_ids(self):
    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    pq.encounter_constraints(location_ids=['loc6', 'loc5'])
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df)


if __name__ == '__main__':
  unittest.main()
