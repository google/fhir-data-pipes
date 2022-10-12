# Copyright 2022 Google LLC
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

import abc
import unittest
import query_lib as ql


_BIGQUERY_DATASET = "synthea_big_r4"
_PROJECT_NAME = "fhir-analytics-test"
_CODE_SYSTEM = "http://www.ampathkenya.org"

_SPARK_BASE_DIR = "./test_files/parquet_big_db_r4"

class _PatientQueryTest:
    """
    Base class that holds all actual tests that generic to all query lib implementations.
    """

    @classmethod
    @abc.abstractmethod
    def get_patient_query_instance(cls):
        return None

    def test_encounter_basic_query(self):
        pq = self.get_patient_query_instance()
        actual_df = pq.get_patient_encounter_view(
            force_location_type_columns=True
        )
        self.assertEqual(62833, len(actual_df))
        expected_cols = sorted(
            [
                "encPatientId",
                "locationId",
                "encTypeSystem",
                "encTypeCode",
                "locationDisplay",
                "num_encounters",
                "firstDate",
                "lastDate",
            ]
        )
        actual_cols = sorted(actual_df.columns.to_list())
        self.assertListEqual(expected_cols, actual_cols)

    def test_encounter_basic_query_with_system(self):
        pq = self.get_patient_query_instance()
        pq.encounter_constraints(
            typeSystem="http://fhir.openmrs.org/code-system/encounter-type"
        )
        actual_df = pq.get_patient_encounter_view(
            force_location_type_columns=False
        )
        self.assertSetEqual(
            set(actual_df["encTypeSystem"]),
            {"http://fhir.openmrs.org/code-system/encounter-type"},
        )

    def test_encounter_basic_query_with_codes(self):
        pq = self.get_patient_query_instance()
        test_codes = [
            "e22e39fd-7db2-45e7-80f1-60fa0d5a4378",
            "181820aa-88c9-479b-9077-af92f5364329",
        ]

        pq.encounter_constraints(typeCode=test_codes)

        actual_df = pq.get_patient_encounter_view(
            force_location_type_columns=False
        )

        self.assertSetEqual(set(actual_df["encTypeCode"]), set(test_codes))

    def test_encounter_basic_query_with_location_ids(self):
        pq = self.get_patient_query_instance()
        test_locations = ["2131aff8-2e2a-480a-b7ab-4ac53250262b"]
        pq.encounter_constraints(locationId=test_locations)
        actual_df = pq.get_patient_encounter_view(
            force_location_type_columns=False,
            sample_count=10,
        )
        self.assertSetEqual(set(actual_df["locationId"]), set(test_locations))

    def test_encounter_compound_query_params(self):
        test_locations = ["b1a8b05e-3542-4037-bbd3-998ee9c40574"]
        test_codes = [
            "e22e39fd-7db2-45e7-80f1-60fa0d5a4378",
            "181820aa-88c9-479b-9077-af92f5364329",
        ]
        test_type_system = "http://fhir.openmrs.org/code-system/encounter-type"

        pq = self.get_patient_query_instance()
        pq.encounter_constraints(
            typeSystem=test_type_system,
            locationId=test_locations,
            typeCode=test_codes,
        )
        actual_df = pq.get_patient_encounter_view(
            force_location_type_columns=False,
            sample_count=10,
        )

        self.assertSetEqual(set(actual_df["locationId"]), set(test_locations))
        self.assertSetEqual(set(actual_df["encTypeSystem"]), {test_type_system})
        self.assertSetEqual(set(actual_df["encTypeCode"]), set(test_codes))

    def test_obs_basic_query_simple(self):
        pq = self.get_patient_query_instance()

        pq.include_obs_in_value_and_time_range("1111", max_time="2011-01-01")
        pq.include_obs_values_in_time_range("1284", values=["130"])
        actual_df = pq.get_patient_obs_view(sample_count=10)

        expected_columns = {
            "patientId",
            "code",
            "birthDate",
            "gender",
            "num_obs",
            "min_value",
            "max_value",
            "min_date",
            "max_date",
            "last_value",
            "first_value",
            "last_value_code",
            "first_value_code",
        }

        self.assertSetEqual(set(actual_df.columns), expected_columns)

    def test_obs_basic_query_include_all_other_codes(self):
        end_date = "2016-01-01"
        start_date = "2000-01-01"

        # Creating a new `patient_query` to drop all previous constraints
        # and recreate flat views.
        patient_query = self.get_patient_query_instance()
        patient_query.include_all_other_codes(
            min_time=start_date, max_time=end_date
        )
        patient_query.include_obs_in_value_and_time_range(
            "1111", max_time="2011-01-01"
        )

        agg_df = patient_query.get_patient_obs_view()
        self.assertTrue(
            agg_df[agg_df["code"] == "1111"]["max_date"].max() < "2011-01-01",
        )
        self.assertTrue(
            agg_df[agg_df["code"] != "1111"]["max_date"].max() < end_date
        )
        self.assertTrue(
            agg_df[agg_df["code"] != "1111"]["min_date"].min() > start_date
        )

    def test_obs_query_type_codes(self):
        _VL_CODE = "856"  # HIV VIRAL LOAD
        _ARV_PLAN = "1255"  # ANTIRETROVIRAL PLAN
        end_date = "2017-01-01"
        start_date = "2000-01-01"

        # Creating a new `patient_query` to drop all previous constraints
        # and recreate flat views.
        patient_query = self.get_patient_query_instance()
        patient_query.include_obs_values_in_time_range(
            _VL_CODE, min_time=start_date, max_time=end_date
        )
        patient_query.include_obs_values_in_time_range(
            _ARV_PLAN, min_time=start_date, max_time=end_date
        )

        other_end_date = "2020-01-01"
        other_start_date = "1998-01-01"
        patient_query.include_all_other_codes(
            min_time=other_start_date, max_time=other_end_date
        )

        patient_query.encounter_constraints(
            locationId=["2131aff8-2e2a-480a-b7ab-4ac53250262b"]
        )

        agg_df = patient_query.get_patient_obs_view()

        self.assertTrue(
            agg_df[agg_df["code"] == _ARV_PLAN]["max_date"].max() < end_date
        )
        self.assertTrue(
            agg_df[agg_df["code"] == _ARV_PLAN]["min_date"].min() > start_date
        )

        self.assertTrue(
            agg_df[agg_df["code"] != _ARV_PLAN]["max_date"].max()
            < other_end_date
        )
        self.assertTrue(
            agg_df[agg_df["code"] != _ARV_PLAN]["min_date"].min()
            > other_start_date
        )


class PatientQueryTestBigQuery(unittest.TestCase, _PatientQueryTest):
    """
    Test PatientQuery API using the concreate implemenation for BigQuery
    """

    @classmethod
    def get_patient_query_instance(cls):
        return ql.patient_query_factory(
            ql.Runner.BIG_QUERY,
            _BIGQUERY_DATASET,
            _CODE_SYSTEM,
            project_name=_PROJECT_NAME,
        )


class PatientQueryTestSpark(unittest.TestCase, _PatientQueryTest):
    """
    Test PatientQuery API using the concreate implemenation for SPARK
    """

    @classmethod
    def get_patient_query_instance(cls):
        return ql.patient_query_factory(
            ql.Runner.SPARK,
            _SPARK_BASE_DIR,
            _CODE_SYSTEM,
        )


if __name__ == "__main__":
    unittest.main()
