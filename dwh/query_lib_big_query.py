"""
Module that implements Query Engine for BigQuery
"""
import typing as tp

from query_lib import PatientQuery, _EncounterContraints, _ObsConstraints


class _BigQueryEncounterConstraints:
    """
    Encounter constraints helper class that will be set up for querying BigQuery
    """

    def __init__(
        self,
        location_ids: tp.Optional[tp.Iterable[str]] = None,
        type_system: tp.Optional[str] = None,
        type_codes: tp.Optional[tp.Iterable[str]] = None,
    ):
        self.location_ids = location_ids
        self.type_system = type_system
        self.type_codes = type_codes

    def has_location(self) -> bool:
        return self._location_id != None

    def has_type(self) -> bool:
        return (self._type_code != None) or (self._type_system != None)

    def sql(self) -> str:
        """This creates a constraint string with WHERE syntax in SQL."""
        loc_str = "TRUE"
        if self._location_id:
            temp_str = ",".join(['"{}"'.format(v) for v in self._location_id])
            loc_str = "locationId IN ({})".format(temp_str)
        type_code_str = "TRUE"
        if self._type_code:
            temp_str = ",".join(['"{}"'.format(v) for v in self._type_code])
            type_code_str = "encTypeCode IN ({})".format(temp_str)
        type_sys_str = (
            'encTypeSystem="{}"'.format(self._type_system)
            if self._type_system
            else "TRUE"
        )
        return "{} AND {} AND {}".format(loc_str, type_code_str, type_sys_str)


class _BigQueryObsConstraints(_ObsConstraints):
    """
    Observation constraints helper class for querying Big Query
    """

    @staticmethod
    def time_constraint(min_time: str = None, max_time: str = None):
        if not min_time and not max_time:
            return "TRUE"
        cl = []
        if min_time:
            cl.append('obs_effecitve_datetime >= "{}"'.format(min_time))
        if max_time:
            cl.append('obs_effective_datetime <= "{}"'.format(max_time))
        return " AND ".join(cl)

    def _build_critera(self):
        """
        Build obs criteria
        """
        cl = [self.time_constraint(self._min_time, self._max_time)]
        cl.append('obs_code_coding_code="{}"'.format(self._code))
        # We don't need to filter coding.system as it is already done in flattening.
        if self._values:
            codes_str = ",".join(['"{}"'.format(v) for v in self._values])
            cl.append("OC.code IN ({})".format(codes_str))
            # TODO(gdevanla): This is already applied as part of patient._code_system
            # cl.append('OVC.system {}'.format(self._sys_str))
        elif self._min_value or self._max_value:
            if self._min_value:
                cl.append(" obs_value_quantity >= {} ".format(self._min_value))
            if self._max_value:
                cl.append(" obs_value_quantity <= {} ".format(self._max_value))
        return "({})".format(" AND ".join(cl))


class _BigQueryPatientQuery(PatientQuery):
    """
    Concrete implementation of PatientQuery class that serves data stored in BigQuery
    """

    def __init__(self, bq_dataset: str, code_system: str):
        super().__init__(
            code_system, _BigQueryEncounterConstraints, _BigQueryObsConstraints
        )
        self._bq_dataset = bq_dataset

    @classmethod
    def _build_encounter_query(
        cls,
        *,
        bq_dataset: str,
        base_url: str,
        table_name: str,
        location_ids: tp.Optional[tp.Iterable[str]],
        type_system: tp.Optional[str] = None,
        type_codes: tp.Optional[tp.Iterable[str]] = None,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None
    ):
        """
        Helper function to build the sql query which will only query the Encounter table
        Sample Query:
            WITH S AS (
            select * from `learnbq-345320.fhir_sample.encounter`
            )
            select S.id as encounterId,
            S.subject.PatientId as encPatientId,
            S.period.start as first,
            S.period.end as last,
            C.system, C.code,
            L.location.LocationId, L.location.display
            from S, unnest(s.type) as T, unnest(T.coding) as C left join unnest(s.location) as L
            where C.system = 'system3000' and C.code = 'code3000'
            and L.location.locationId in ('test')
        """

        sql_template = """
    WITH S AS (
          select * from {data_set}.{table_name}
          )
          select replace(S.id, '{base_url}', '') as encounterId,
          S.subject.PatientId as encPatientId,
          C.system, C.code,
          L.location.LocationId, L.location.display,
          MIN(S.period.start) as first,
          MAX(S.period.end) as last,
          COUNT(*) as num_encounters
          from S, unnest(s.type.array) as T, unnest(T.coding.array) as C left join unnest(s.location.array) as L
          --C.system = 'system3000' and C.code = 'code3000'
          --and L.location.locationId in ('test')
          {where}
          group by S.id, S.subject.PatientId, C.system, C.code, L.location.LocationId, L.location.display
          {sample_count}
    """

        clause_location_id = None
        if location_ids:
            clause_location_id = "L.location.locationId in ({})".format(
                _build_in_list_with_quotes(location_ids)
            )
        clause_type_system = None
        if type_system:
            clause_type_system = "C.system = '{}'".format(type_system)
        clause_type_codes = None
        if type_codes:
            clause_type_codes = "C.code in ({})".format(
                _build_in_list_with_quotes(type_codes)
            )

        where_clause = " and ".join(
            x for x in [clause_location_id, clause_type_system, clause_type_codes] if x
        )
        if where_clause:
            where_clause = " where " + where_clause
        sql = sql_template.format(
            table_name=table_name,
            base_url=base_url,
            data_set=bq_dataset,
            where=where_clause,
            sample_count="" if sample_count is None else "LIMIT " + str(sample_count),
        )
        return sql

    def _build_obs_encounter_query(
        self, dataset: str, sample_count: tp.Optional[int] = None
    ) -> str:

        """
        Helper functions that builds the query to get patient_observation data
        Args:
        dataset:
        sample_count:
        """

        sql = """
    with O as
    (select * from `{dataset}.Observation`),
      O1 as
      (select
          OC.system obs_system,
          OC.code as obs_code_coding_code,
          OVC.code obs_value_code,
          O.effective.dateTime obs_effective_datetime,
          O.value.quantity.value as obs_value_quantity,
          O.subject.PatientId obs_subject_patient_id,
          O.context.encounterId obs_context_encounter_id,
          FORMAT('%s,%s', cast(O.effective.dateTime as string) , cast(O.value.quantity.value as string)) as date_and_value,
          FORMAT('%s,%s', cast(O.effective.dateTime as string), OVC.code) as date_and_value_code,
          from O, unnest(O.code.coding.array) as OC, unnest(O.value.codeableConcept.coding.array) as OVC
          where TRUE and {code_coding_sytem_str} and {value_codeable_coding_system}
          and {all_obs_constraints}
          --OC.system = 'http://www.ampathkenya.org'
          --and OVC.system is not null
          ),
      E  AS (
            select * from `{dataset}.Encounter`
            ),
      E1 AS (
          select E.id as encounterId, C.system, C.code,
            L.location.LocationId, L.location.display,
            from E, unnest(E.type.array) as T, unnest(T.coding.array) as C left join unnest(E.location.array) as L),
      G as (select
          obs_subject_patient_id patient_id,
          obs_code_coding_code as coding_code, -- TODO: Should this be coding.code
          count(*) as num_obs,
          min(obs_value_quantity) as min_value,
          max(obs_value_quantity) as max_value,
          min(obs_effective_datetime) as min_date,
          max(obs_effective_datetime) as max_date,
          min(date_and_value) as min_date_value,
          max(date_and_value) as max_date_value,
          min(date_and_value_code) as min_date_value_code,
          max(date_and_value_code) as max_date_value_code,
        from O1 inner join E1 on E1.encounterId = O1.obs_context_encounter_id
        group by patient_id, coding_code)
      select patient_id as patientId, coding_code,
      P.birthDate as birthDate,
      P.gender as gender,
      num_obs, min_value, max_value, min_date, max_date, min_date_value, max_date_value, min_date_value_code, max_date_value_code
      from G inner join `{dataset}.Patient` P on G.patient_id = P.id
    """

        # TODO(gdevanla): Yet to add encounter constraints

        code_coding_system_str = (
            " OC.system is null "
            if not self._code_system
            else " OC.system = {} ".format(self._code_system)
        )
        value_codeable_coding_system_str = (
            " OVC.system is null "
            if not self._code_system
            else " OVC.system = {} ".format(self._code_system)
        )

        all_obs_constraints = self._all_obs_constraints()

        sql = sql.format(
            dataset=dataset,
            code_coding_system_str=code_coding_system_str,
            value_codeable_coding_system_str=value_codeable_coding_system_str,
            all_obs_constraints=all_obs_constraints,
            sample_count="" if sample_count is None else "LIMIT " + str(sample_count),
        )
        return sql

    def _all_obs_constraints(self) -> str:

        if not self._code_constraint and not self._include_all_codes:
            raise ValueError(
                "Either obs constraints need to be set of include_all_codes need to be set"
            )

        if self._include_all_codes and not self._code_constraints:
            return " TRUE "
        constraints_str = " OR ".join(
            [obs_constraint.sql() for obs_constraint in self._code_constraint.values()]
        )
        if not self._include_all_codes:
            return "({})".format(constraints_str)
        others_str = " AND ".join(
            ['OVC.code != "{}"'.format(code) for code in self._code_constraint]
            + [
                _ObsConstraints.time_constraint(
                    self._all_codes_min_time, self._all_codes_max_time
                )
            ]
        )
        return " ({} OR ({})) ".format(constraints_str, others_str)

    def get_patient_encounter_view(
        self,
        base_url: str,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None,
    ) -> pandas.DataFrame:

        sql = self._build_encounter_query(
            bq_dataset=self._bq_dataset,
            table_name="Encounter",
            base_url=base_url,
            location_ids=self._enc_constraint.location_ids
            if self._enc_constraint
            else None,
            type_system=self._enc_constraint.type_system
            if self._enc_constraint
            else None,
            type_codes=self._enc_constraint.type_codes
            if self._enc_constraint
            else None,
            force_location_type_columns=force_location_type_columns,
            sample_count=sample_count,
        )

        with bigquery.Client() as client:
            patient_enc = client.query(sql).to_dataframe()
            return patient_enc

    def get_patient_obs_view(
        self,
        base_url: str,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None,
    ) -> pandas.DataFrame:

        sql = self._build_obs_encounter_query(
            dataset=self._bq_dataset, sample_count=sample_count
        )

        with bigquery.Client() as client:
            patient_obs_enc = client.query(sql).to_dataframe()
            return patient_obs_enc
