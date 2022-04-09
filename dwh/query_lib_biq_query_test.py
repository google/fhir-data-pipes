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
import query_lib_big_query as ql

"""
For these tests to run the Google Auth settings have to be run on the machine running these tests:

Follow instructions here https://cloud.google.com/sdk/gcloud/reference/auth to

a. gcloud auth login
b. gcloud config set project <project_name>
  (project_name here is the project that has the dataset 'synthea_big'
   that is used by tests)
"""

_BIGQUERY_DATASET = 'synthea_big'
_CODE_SYSTEM = 'http://www.ampathkenya.org'

class BigQueryPatientQueryTest(unittest.TestCase):

  def test_encounter_basic_query(self):

    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df.head(2).T)

  def test_encounter_basic_query_with_system(self):

    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    pq.encounter_constraints(
        type_system='http://fhir.openmrs.org/code-system/encounter-type')
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df)

  def test_encounter_basic_query_with_codes(self):
    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    pq.encounter_constraints(
        type_codes=['5021b1a1-e7f6-44b4-ba02-da2f2bcf8718'])
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False
    )
    print(actual_df)

  def test_encounter_basic_query_with_location_ids(self):
    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system='dummy_code_system'
    )
    pq.encounter_constraints(
        location_ids=['2131aff8-2e2a-480a-b7ab-4ac53250262b'])
    actual_df = pq.get_patient_encounter_view(
        base_url='', force_location_type_columns=False,
        sample_count=10,
    )
    print(actual_df)

  def test_obs_basic_query(self):

    pq = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system=_CODE_SYSTEM)

    #pq.include_all_other_codes(True, '2011-01-01')
    pq.include_obs_in_value_and_time_range('844', max_time='2011-01-01', max_val=10)
    #pq.include_obs_values_in_time_range('1284', values=['130'])
    pq.include_obs_values_in_time_range('1284', values=['130'])
    actual_df = pq.get_patient_obs_view(
        base_url='', force_location_type_columns=False,
        sample_count=10
    )
    print(actual_df.iloc[:2].T)
    print(actual_df)

  def test_obs_query_1(self):
    _VL_CODE = '856'  # HIV VIRAL LOAD
    _ARV_PLAN = '1255'  # ANTIRETROVIRAL PLAN
    end_date='2018-01-01'
    start_date='1998-01-01'
    old_start_date='1978-01-01'
    _BASE_URL = ''

    # Creating a new `patient_query` to drop all previous constraints
    # and recreate flat views.
    patient_query = ql._BigQueryPatientQuery(
        bq_dataset=_BIGQUERY_DATASET, code_system=_CODE_SYSTEM)

    # patient_query.include_obs_values_in_time_range(
    #     _VL_CODE, min_time=start_date, max_time=end_date)
    # patient_query.include_obs_values_in_time_range(
    #     _ARV_PLAN, min_time=start_date, max_time=end_date)
    patient_query.include_all_other_codes(min_time=start_date, max_time=end_date)

    patient_query.encounter_constraints(
        locationId=['2131aff8-2e2a-480a-b7ab-4ac53250262b'])
    #patient_query.include_all_other_codes(min_time=start_date, max_time=end_date)
    patient_query.include_obs_values_in_time_range('1271')
    patient_query.include_obs_values_in_time_range('1265', max_time='2010-07-10')
    # 2131aff8-2e2a-480a-b7ab-4ac53250262b

    # Note the first call to `find_patient_aggregates` starts a local Spark
    # cluster, load input files, and flattens observations. These won't be
    # done in subsequent calls of this function on the same instance.
    # Also same cluster will be reused for other instances of `PatientQuery`.
    agg_df = patient_query.get_patient_obs_view(_BASE_URL)
    print(agg_df.head(10))
    print(agg_df[agg_df['patientId'] == '00c1426f-ca04-414a-8db7-043bb41b64d2'].head())
    print(agg_df[agg_df['patientId'] == '4553cb1b-d318-404d-86cb-595e91d39f46'].head())

if __name__ == '__main__':
  unittest.main()
