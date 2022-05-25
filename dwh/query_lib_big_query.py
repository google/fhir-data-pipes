"""Module that implements Query Engine for BigQuery."""
import typing as tp
import pandas as pd

from base import PatientQuery

_DATE_VALUE_SEPARATOR = ","

try:
    from google.cloud import bigquery
except ImportError:
    pass  # not all set up need to have bigquery libraries installed


def _build_in_list_with_quotes(values: tp.Iterable[tp.Any]):
    """
    Builds the `in` operand for a where clause
    """
    return ",".join(('"{}"'.format(x) for x in values))


class _BigQueryEncounterConstraints:
    """Encounter constraints helper class that will be set up for querying
    BigQuery."""

    def __init__(
        self,
        locationId: tp.List[str] = None,
        typeSystem: str = None,
        typeCode: tp.List[str] = None,
    ):
        self._location_id = locationId
        self._type_system = typeSystem
        self._type_code = typeCode

    def has_location(self) -> bool:
        return self._location_id != None

    def has_type(self) -> bool:
        return (self._type_code != None) or (self._type_system != None)

    def where_clause(self):
        """Builds Encounter criteria.

        Assumes, the query set will be as follows: from S,
        unnest(s.type.array) as T, unnest(T.coding.array) as C left join
        unnest(s.location.array) as L
        """
        clause_location_id = None
        if self._location_id:
            clause_location_id = "L.location.locationId in ({})".format(
                _build_in_list_with_quotes(self._location_id)
            )
        clause_type_system = None
        if self._type_system:
            clause_type_system = "C.system = '{}'".format(self._type_system)
        clause_type_codes = None
        if self._type_code:
            clause_type_codes = "C.code in ({})".format(
                _build_in_list_with_quotes(self._type_code)
            )
        where_clause = " and ".join(
            x for x in [clause_location_id, clause_type_system, clause_type_codes] if x
        )
        if where_clause:
            return " where {} ".format(where_clause)
        return ""


class _BigQueryObsConstraints:
    """Observation constraints helper class for querying Big Query."""

    def __init__(
        self,
        code: str,
        values: tp.List[str] = None,
        value_sys: str = None,
        min_value: float = None,
        max_value: float = None,
        min_time: str = None,
        max_time: str = None,
    ) -> None:

        self._code = code
        self._sys_str = '="{}"'.format(value_sys) if value_sys else "IS NULL"
        self._values = values
        self._min_time = min_time
        self._max_time = max_time
        self._min_value = min_value
        self._max_value = max_value

    @staticmethod
    def time_constraint(min_time: str = None, max_time: str = None):
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

    def where_clause(self):
        """Build obs criteria."""
        conditions = [self.time_constraint(self._min_time, self._max_time)]
        conditions.append(' OC.code = "{}" '.format(self._code))
        # We don't need to filter coding.system as it is already done in
        # flattening.
        if self._values:
            codes_str = ",".join(['"{}"'.format(v) for v in self._values])
            conditions.append("OVC.code IN ({})".format(codes_str))
            # TODO(gdevanla): This is already applied as part of patient._code_system
            # conditions.append('OVC.system {}'.format(self._sys_str))
        elif self._min_value or self._max_value:
            if self._min_value:
                conditions.append(
                    " O.value.quantity.value >= {} ".format(self._min_value)
                )
            if self._max_value:
                conditions.append(
                    " O.value.quantity.value <= {} ".format(self._max_value)
                )
        return "({})".format(" AND ".join(conditions))


class _BigQueryPatientQuery(PatientQuery):
    """Concrete implementation of PatientQuery class that serves data stored in
    BigQuery."""

    def __init__(self, project_name: str, bq_dataset: str, code_system: str):
        super().__init__(
            code_system, _BigQueryEncounterConstraints, _BigQueryObsConstraints
        )
        self._bq_dataset = bq_dataset
        self._project_name = project_name

    def _build_encounter_query(
        self,
        *,
        bq_dataset: str,
        base_url: str,
        table_name: str,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None
    ) -> str:
        """Helper function to build the sql query which will only query the
        """

        sql_template = """
    WITH S AS (
          SELECT * FROM {data_set}.{table_name}
          )
          SELECT
          S.subject.PatientId AS encPatientId,
          L.location.LocationId AS locationId,
          C.system AS encTypeSystem,
          C.code AS encTypeCode,
          L.location.display AS locationDisplay,
          COUNT(*) AS num_encounters,
          MIN(S.period.start) AS firstDate,
          MAX(S.period.end) AS lastDate
          from S, UNNEST(s.type.array) AS T, UNNEST(T.coding.array) AS C LEFT JOIN UNNEST(s.location.array) AS L
          {where}
          GROUP BY S.subject.PatientId, L.location.LocationId, L.location.display, C.system, C.code
          {sample_count}
    """

        where_clause = self._enc_constraint.where_clause()
        sql = sql_template.format(
            table_name=table_name,
            base_url=base_url,
            data_set=bq_dataset,
            where=where_clause,
            sample_count="" if sample_count is None else "LIMIT " + str(sample_count),
        )
        return sql

    def _build_obs_encounter_query(
        self, dataset: str, base_url: str, sample_count: tp.Optional[int] = None
    ) -> str:
        """
        Helper functions that builds the query to get patient_observation data
        Args:
            dataset: Indicates the schema to use.
            sample_count: Restricts returned number of rows to sample count.
                          Useful for testing and exploration.
        """

        sql_template = """
    with O AS
    (SELECT * FROM `{dataset}.Observation`),
      O1 as
      (SELECT
          OC.system obs_system,
          OC.code AS obs_code_coding_code,
          OVC.code obs_value_code,
          O.effective.dateTime obs_effective_datetime,
          O.value.quantity.value AS obs_value_quantity,
          O.subject.PatientId obs_subject_patient_id,
          O.context.encounterId obs_context_encounter_id,
          FORMAT('%s,%s', cast(O.effective.dateTime AS string),
                 cast(O.value.quantity.value AS string)) AS date_and_value,
          FORMAT('%s,%s', cast(O.effective.dateTime AS string), OVC.code)
                 AS date_and_value_code,
          FROM O LEFT JOIN UNNEST(O.code.coding.array) AS OC LEFT JOIN
          UNNEST(O.value.codeableConcept.coding.array) AS OVC
          where {code_coding_system} and {value_codeable_coding_system}
          and {all_obs_constraints}
          ),
      E1 AS (
          SELECT replace(E.id, '{base_url}', '') AS encounterId, C.system, C.code,
            L.location.LocationId, L.location.display,
            FROM `{dataset}.Encounter` AS E LEFT JOIN UNNEST(E.type.array) AS T LEFT JOIN
            UNNEST(T.coding.array) AS C LEFT JOIN UNNEST(E.location.array) AS L
            {encounter_where_clause}
            ),
      G AS (SELECT
          obs_subject_patient_id patient_id,
          obs_code_coding_code AS coding_code, -- TODO: Should this be coding.code
          count(*) AS num_obs,
          min(obs_value_quantity) AS min_value,
          max(obs_value_quantity) AS max_value,
          min(obs_effective_datetime) AS min_date,
          max(obs_effective_datetime) AS max_date,
          min(date_and_value) AS min_date_value,
          max(date_and_value) AS max_date_value,
          min(date_and_value_code) AS min_date_value_code,
          max(date_and_value_code) AS max_date_value_code,
        FROM O1 inner JOIN E1 on E1.encounterId = O1.obs_context_encounter_id
        group by patient_id, coding_code)
      SELECT patient_id AS patientId, coding_code AS code,
      P.birthDate AS birthDate,
      P.gender AS gender,
      num_obs, min_value, max_value, min_date, max_date, min_date_value,
      max_date_value, min_date_value_code, max_date_value_code
      FROM G inner JOIN `{dataset}.Patient` P on G.patient_id = P.id
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
            sample_count="" if sample_count is None else " LIMIT " + str(sample_count),
            base_url=base_url,
            encounter_where_clause=self._enc_constraint.where_clause(),
        )
        return sql

    def _all_obs_constraints(self) -> str:

        if not self._code_constraint:
            if self._include_all_codes:
                return "TRUE"
            return "FALSE"

        constraints_str = " OR ".join(
            [
                obs_constraint.where_clause()
                for obs_constraint in self._code_constraint.values()
            ]
        )
        if not self._include_all_codes:
            return " ({}) ".format(constraints_str)

        others_str = " AND ".join(
            ['OC.code != "{}"'.format(code) for code in self._code_constraint]
            + [
                self._obs_constraints_class.time_constraint(
                    self._all_codes_min_time, self._all_codes_max_time
                )
            ]
        )
        return " (({}) OR ({})) ".format(constraints_str, others_str)

    def get_patient_encounter_view(
        self,
        base_url: str,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None,
    ) -> pd.DataFrame:

        sql = self._build_encounter_query(
            bq_dataset=self._bq_dataset,
            table_name="Encounter",
            base_url=base_url,
            force_location_type_columns=force_location_type_columns,
            sample_count=sample_count,
        )

        with bigquery.Client(project=self._project_name) as client:
            patient_enc = client.query(sql).to_dataframe()
            return patient_enc

    def get_patient_obs_view(
        self,
        base_url: str,
        sample_count: tp.Optional[int] = None,
    ) -> pd.DataFrame:

        sql = self._build_obs_encounter_query(
            dataset=self._bq_dataset, sample_count=sample_count, base_url=base_url
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
            patient_obs_enc.drop(columns=[col[1] for col in col_map], inplace=True)
            return patient_obs_enc
