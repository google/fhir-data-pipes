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

"""Module that implements Query Engine for BigQuery."""
import typing as tp

import pandas as pd
from google.cloud import bigquery

import base

_DATE_VALUE_SEPARATOR = ","


def _build_in_list_with_quotes(values: tp.List[tp.Any]) -> str:
    """
    Builds the `in` operand for a where clause
    """
    return ",".join(('"{}"'.format(x) for x in values))


class BigQueryPatientQuery(base.PatientQuery):
    """Concrete implementation of PatientQuery class that serves data stored in
    BigQuery."""

    def __init__(self, project_name: str, bq_dataset: str, code_system: str):
        """
        Args:
            project_name: The GoogleCloud project name. This field is required if
                is used as data source.
            bq_dataset: The name of BigQuery dataset.
            code_system: The code system that needs to be used as extra filter
               while querying data.
        """
        super().__init__(code_system)
        self._bq_dataset = bq_dataset
        self._project_name = project_name

    def _build_encounter_query(
        self,
        *,
        bq_dataset: str,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None
    ) -> str:
        """Helper function to build the sql query."""

        sql_template = """
        WITH E AS (
          SELECT * FROM {data_set}.Encounter
          )
        SELECT
        E.subject.PatientId AS encPatientId,
        L.location.LocationId AS locationId,
        C.system AS encTypeSystem,
        C.code AS encTypeCode,
        L.location.display AS locationDisplay,
        COUNT(*) AS num_encounters,
        MIN(E.period.start) AS firstDate,
        MAX(E.period.end) AS lastDate
        FROM E, UNNEST(E.type.array) AS T, UNNEST(T.coding.array) AS C
        LEFT JOIN UNNEST(E.location.array) AS L
        {where}
        GROUP BY E.subject.PatientId, L.location.LocationId,
        L.location.display, C.system, C.code
        {sample_count}
    """

        where_clause = self._construct_encounter_constraint(
            self._enc_constraint
        )
        sql = sql_template.format(
            data_set=bq_dataset,
            where=where_clause,
            sample_count=""
            if sample_count is None
            else "LIMIT " + str(sample_count),
        )
        return sql

    def _build_obs_encounter_query(
        self, dataset: str, sample_count: tp.Optional[int] = None
    ) -> str:
        """
        Helper functions that builds the query to get patient_observation data
        Args:
            dataset: Indicates the schema to use.
            sample_count: Restricts returned number of rows to sample count.
                          Useful for testing and exploration.
        """

        sql_template = """
        WITH O AS
            (SELECT * FROM `{dataset}.Observation`),
        O1 as
            (SELECT
             OC.system obs_system,
             OC.code AS obs_code_coding_code,
             OVC.code obs_value_code,
             O.effective.dateTime obs_effective_datetime,
             O.value.quantity.value AS obs_value_quantity,
             O.subject.PatientId obs_subject_patient_id,
             O.encounter.encounterId obs_context_encounter_id,
             FORMAT('%s,%s', cast(O.effective.dateTime AS string),
                    cast(O.value.quantity.value AS string)) AS date_and_value,
             FORMAT('%s,%s', cast(O.effective.dateTime AS string), OVC.code)
                    AS date_and_value_code,
             FROM O LEFT JOIN UNNEST(O.code.coding.array) AS OC LEFT JOIN
             UNNEST(O.value.codeableConcept.coding.array) AS OVC
             WHERE {code_coding_system} AND {value_codeable_coding_system}
             AND {all_obs_constraints}
         ),
        E1 AS
            (SELECT E.id AS encounterId, C.system, C.code,
            L.location.LocationId, L.location.display,
            FROM `{dataset}.Encounter` AS E LEFT JOIN UNNEST(E.type.array) AS T LEFT JOIN
            UNNEST(T.coding.array) AS C LEFT JOIN UNNEST(E.location.array) AS L
            {encounter_where_clause}
            ),
        G AS
            (SELECT
             obs_subject_patient_id patient_id,
             obs_code_coding_code AS coding_code, -- TODO: Should this be coding.code
             COUNT(*) AS num_obs,
             MIN(obs_value_quantity) AS min_value,
             MAX(obs_value_quantity) AS max_value,
             MIN(obs_effective_datetime) AS min_date,
             MAX(obs_effective_datetime) AS max_date,
             MIN(date_and_value) AS min_date_value,
             MAX(date_and_value) AS max_date_value,
             MIN(date_and_value_code) AS min_date_value_code,
             MAX(date_and_value_code) AS max_date_value_code,
             FROM O1 INNER JOIN E1 ON E1.encounterId = O1.obs_context_encounter_id
             GROUP BY patient_id, coding_code)
         SELECT patient_id AS patientId, coding_code AS code,
         P.birthDate AS birthDate,
         P.gender AS gender,
         num_obs, min_value, max_value, min_date, max_date, min_date_value,
         max_date_value, min_date_value_code, max_date_value_code
         FROM G inner JOIN `{dataset}.Patient` P ON G.patient_id = P.id
         {sample_count}
    """

        # TODO(gdevanla): Yet to add encounter constraints

        code_coding_system_str = (
            " OC.system is null "
            if not self._code_system
            else ' OC.system = "{}" '.format(self._code_system)
        )
        value_codeable_coding_system_str = " O.value is null or " + (
            " OVC.system is null "
            if not self._code_system
            else ' (OVC.system is null or  OVC.system = "{}") '.format(
                self._code_system
            )
        )
        value_codeable_coding_system_str = " ({}) ".format(
            value_codeable_coding_system_str
        )

        all_obs_constraints = self._all_obs_constraints()

        sql = sql_template.format(
            dataset=dataset,
            code_coding_system=code_coding_system_str,
            value_codeable_coding_system=value_codeable_coding_system_str,
            all_obs_constraints=all_obs_constraints,
            sample_count=""
            if sample_count is None
            else " LIMIT " + str(sample_count),
            encounter_where_clause=self._construct_encounter_constraint(
                self._enc_constraint
            ),
        )
        return sql

    def _all_obs_constraints(self) -> str:
        if not self._code_constraint:
            if self._include_all_codes:
                return "TRUE"
            return "FALSE"

        constraints_str = " OR ".join(
            [
                self._construct_obs_constraint(obs_constraint)
                for obs_constraint in self._code_constraint.values()
            ]
        )
        if not self._include_all_codes:
            return " ({}) ".format(constraints_str)

        others_str = " AND ".join(
            ['OC.code != "{}"'.format(code) for code in self._code_constraint]
            + [
                self._time_constraint(
                    self._all_codes_min_time, self._all_codes_max_time
                )
            ]
        )
        return " (({}) OR ({})) ".format(constraints_str, others_str)

    def get_patient_encounter_view(
        self,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None,
    ) -> pd.DataFrame:
        sql = self._build_encounter_query(
            bq_dataset=self._bq_dataset,
            force_location_type_columns=force_location_type_columns,
            sample_count=sample_count,
        )

        with bigquery.Client(project=self._project_name) as client:
            patient_enc = client.query(sql).to_dataframe()
            return patient_enc

    def get_patient_obs_view(
        self,
        sample_count: tp.Optional[int] = None,
    ) -> pd.DataFrame:
        sql = self._build_obs_encounter_query(
            dataset=self._bq_dataset,
            sample_count=sample_count,
        )

        with bigquery.Client(project=self._project_name) as client:
            patient_obs_enc = client.query(sql).to_dataframe()
            col_map = (
                ("last_value", "max_date_value"),
                ("first_value", "min_date_value"),
                ("last_value_code", "max_date_value_code"),
                ("first_value_code", "min_date_value_code"),
            )

            for dest_col, source_col in col_map:
                patient_obs_enc[dest_col] = patient_obs_enc[source_col].apply(
                    lambda x: None if x is None else x.split(",")[1]
                )
            patient_obs_enc.drop(
                columns=[col[1] for col in col_map], inplace=True
            )
            return patient_obs_enc

    @staticmethod
    def _construct_encounter_constraint(
        enc_constraint: base.EncounterConstraints,
    ):
        """Builds Encounter criteria.

        Assumes, the query set will be as follows: from S,
        unnest(s.type.array) as T, unnest(T.coding.array) as C left join
        unnest(s.location.array) as L
        """
        clause_location_id = None
        if enc_constraint.location_id:
            clause_location_id = "L.location.locationId in ({})".format(
                _build_in_list_with_quotes(enc_constraint.location_id)
            )
        clause_type_system = None
        if enc_constraint.type_system:
            clause_type_system = "C.system = '{}'".format(
                enc_constraint.type_system
            )
        clause_type_codes = None
        if enc_constraint.type_code:
            clause_type_codes = "C.code in ({})".format(
                _build_in_list_with_quotes(enc_constraint.type_code)
            )
        where_clause = " and ".join(
            x
            for x in [clause_location_id, clause_type_system, clause_type_codes]
            if x
        )
        if where_clause:
            return " where {} ".format(where_clause)
        return ""

    @staticmethod
    def _time_constraint(min_time: str = None, max_time: str = None):
        """
        Build time constraints for Observation queries
        """
        if not min_time and not max_time:
            return " TRUE "
        conditions = []
        if min_time:
            conditions.append('O.effective.dateTime >= "{}"'.format(min_time))
        if max_time:
            conditions.append('O.effective.dateTime <= "{}"'.format(max_time))
        return " AND ".join(conditions)

    def _construct_obs_constraint(self, obs_constraint: base.ObsConstraints):
        """Build obs criteria."""
        conditions = [
            self._time_constraint(
                obs_constraint.min_time, obs_constraint.max_time
            )
        ]
        conditions.append(' OC.code = "{}" '.format(obs_constraint.code))
        # We don't need to filter coding.system as it is already done in
        # flattening.
        if obs_constraint.values:
            codes_str = ",".join(
                ['"{}"'.format(v) for v in obs_constraint.values]
            )
            conditions.append("OVC.code IN ({})".format(codes_str))
        elif obs_constraint.min_value or obs_constraint.max_value:
            if obs_constraint.min_value:
                conditions.append(
                    " O.value.quantity.value >= {} ".format(
                        obs_constraint.min_value
                    )
                )
            if obs_constraint.max_value:
                conditions.append(
                    " O.value.quantity.value <= {} ".format(
                        obs_constraint.max_value
                    )
                )
        return "({})".format(" AND ".join(conditions))
