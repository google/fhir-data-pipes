"""Module that implements Query Engine for BigQuery."""
import typing as tp
import pandas as pd

from query_lib import _EncounterContraints
from query_lib import _ObsConstraints
from query_lib import PatientQuery

_DATE_VALUE_SEPARATOR = ','

try:
  from google.cloud import bigquery
except ImportError:
  pass  # not all set up need to have bigquery libraries installed


def _build_in_list_with_quotes(values: tp.Iterable[tp.Any]):
  return ",".join(map(lambda x: '\"{}\"'.format(x), values))


class _BigQueryEncounterConstraints(_EncounterContraints):
  """Encounter constraints helper class that will be set up for querying BigQuery.
  """

  def sql(self):
    """
    Builds Encounter criteria. Assumes, the query set will be as follows:
    `from S, unnest(s.type.array) as T, unnest(T.coding.array) as C left join unnest(s.location.array) as L`
    """
    clause_location_id = None
    if self._location_id:
      clause_location_id = 'L.location.locationId in ({})'.format(
          _build_in_list_with_quotes(self._location_id)
      )
    clause_type_system = None
    if self._type_system:
      clause_type_system = "C.system = '{}'".format(self._type_system)
    clause_type_codes = None
    if self._type_code:
      clause_type_codes = 'C.code in ({})'.format(
          _build_in_list_with_quotes(self._type_code)
      )
    where_clause = ' and '.join(
        x for x in [clause_location_id, clause_type_system, clause_type_codes]
        if x
    )
    if where_clause:
      return ' where {} '.format(where_clause)
    return ''


class _BigQueryObsConstraints(_ObsConstraints):
  """Observation constraints helper class for querying Big Query."""

  @staticmethod
  def time_constraint(min_time: str = None, max_time: str = None):
    if not min_time and not max_time:
      return ' TRUE '
    cl = []
    if min_time:
      cl.append('O.effective.dateTime >= "{}"'.format(min_time))
    if max_time:
      cl.append('O.effective.dateTime <= "{}"'.format(max_time))
    return ' AND '.join(cl)

  def _build_critera(self):
    """Build obs criteria."""
    cl = [self.time_constraint(self._min_time, self._max_time)]
    cl.append(' OC.code = "{}" '.format(self._code))
    # We don't need to filter coding.system as it is already done in
    # flattening.
    if self._values:
      codes_str = ','.join(['"{}"'.format(v) for v in self._values])
      cl.append('OVC.code IN ({})'.format(codes_str))
      # TODO(gdevanla): This is already applied as part of patient._code_system
      # cl.append('OVC.system {}'.format(self._sys_str))
    elif self._min_value or self._max_value:
      if self._min_value:
        cl.append(' O.value.quantity.value >= {} '.format(self._min_value))
      if self._max_value:
        cl.append(' O.value.quantity.value <= {} '.format(self._max_value))
    return '({})'.format(' AND '.join(cl))


class _BigQueryPatientQuery(PatientQuery):
  """
  Concrete implementation of PatientQuery class that serves data stored in BigQuery.
  """

  def __init__(self, bq_dataset: str, code_system: str):
    super().__init__(
        code_system, _BigQueryEncounterConstraints, _BigQueryObsConstraints
    )
    self._bq_dataset = bq_dataset

  def _build_encounter_query(
      self,
      *,
      bq_dataset: str,
      base_url: str,
      table_name: str,
      force_location_type_columns: bool = True,
      sample_count: tp.Optional[int] = None
  ):
    """
    Helper function to build the sql query which will only query the
        Encounter table Sample Query: WITH S AS (

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
          select
          --replace(S.id, '{base_url}', '') as encounterId,
          S.subject.PatientId as encPatientId,
          L.location.LocationId as locationId,
          C.system as encTypeSystem,
          C.code as encTypeCode,
          L.location.display as locationDisplay,
          COUNT(*) as num_encounters,
          MIN(S.period.start) as firstDate,
          MAX(S.period.end) as lastDate
          from S, unnest(s.type.array) as T, unnest(T.coding.array) as C left join unnest(s.location.array) as L
          --C.system = 'system3000' and C.code = 'code3000'
          --and L.location.locationId in ('test')
          {where}
          group by S.id, S.subject.PatientId, C.system, C.code, L.location.LocationId, L.location.display
          {sample_count}
    """

    where_clause = self._enc_constraint.sql()
    sql = sql_template.format(
        table_name=table_name,
        base_url=base_url,
        data_set=bq_dataset,
        where=where_clause,
        sample_count='' if sample_count is None else 'LIMIT ' +
        str(sample_count),
    )
    return sql

  def _build_obs_encounter_query(
      self,
      dataset: str,
      base_url: str,
      sample_count: tp.Optional[int] = None
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
          from O left join unnest(O.code.coding.array) as OC LEFT JOIN unnest(O.value.codeableConcept.coding.array) as OVC
          where TRUE and {code_coding_system} and {value_codeable_coding_system}
          and {all_obs_constraints}
          --OC.system = 'http://www.ampathkenya.org'
          --and OVC.system is not null
          ),
      E  AS (
            select * from `{dataset}.Encounter`
            ),
      E1 AS (
          select replace(E.id, '{base_url}', '') as encounterId, C.system, C.code,
            L.location.LocationId, L.location.display,
            from E left join unnest(E.type.array) as T left join unnest(T.coding.array) as C left join unnest(E.location.array) as L
            {encounter_where_clause}
            ),
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
      select patient_id as patientId, coding_code as code,
      P.birthDate as birthDate,
      P.gender as gender,
      num_obs, min_value, max_value, min_date, max_date, min_date_value, max_date_value, min_date_value_code, max_date_value_code
      from G inner join `{dataset}.Patient` P on G.patient_id = P.id
      {sample_count}
    """

    # TODO(gdevanla): Yet to add encounter constraints

    code_coding_system_str = (
        ' OC.system is null ' if not self._code_system else
        ' OC.system = "{}" '.format(self._code_system)
    )
    value_codeable_coding_system_str = ' O.value is null or ' + (
        ' OVC.system is null ' if not self._code_system else
        ' OVC.system = "{}" '.format(self._code_system)
    )

    all_obs_constraints = self._all_obs_constraints()

    sql = sql.format(
        dataset=dataset,
        code_coding_system=code_coding_system_str,
        value_codeable_coding_system=value_codeable_coding_system_str,
        all_obs_constraints=all_obs_constraints,
        sample_count='' if sample_count is None else ' LIMIT ' +
        str(sample_count),
        base_url=base_url,
        encounter_where_clause=self._enc_constraint.sql()
    )
    return sql

  def _all_obs_constraints(self) -> str:

    if not self._code_constraint:
      if self._include_all_codes:
        return 'TRUE'
      else:
        return 'FALSE'

    constraints_str = ' OR '.join([
        obs_constraint._build_critera()
        for obs_constraint in self._code_constraint.values()
    ])
    if not self._include_all_codes:
      return " ({}) ".format(constraints_str)

    others_str = ' AND '.join(
        ['OC.code != "{}"'.format(code) for code in self._code_constraint] + [
            self._obs_constraint_class.
            time_constraint(self._all_codes_min_time, self._all_codes_max_time)
        ]
    )
    return ' (({}) OR ({})) '.format(constraints_str, others_str)

  def get_patient_encounter_view(
      self,
      base_url: str,
      force_location_type_columns: bool = True,
      sample_count: tp.Optional[int] = None,
  ) -> pd.DataFrame:

    sql = self._build_encounter_query(
        bq_dataset=self._bq_dataset,
        table_name='Encounter',
        base_url=base_url,
        force_location_type_columns=force_location_type_columns,
        sample_count=sample_count,
    )

    with bigquery.Client() as client:
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

    print(sql)

    with bigquery.Client() as client:
        patient_obs_enc = client.query(sql).to_dataframe()
        print(patient_obs_enc[['max_date_value', 'min_date_value', 'max_date_value_code', 'min_date_value_code']])


        col_map = (('last_value', 'max_date_value'),
                   ('first_value', 'min_date_value'),
                   ('last_value_code', 'max_date_value_code'),
                   ('first_value_code', 'min_date_value_code'))

        for dest_col, source_col in col_map:
            patient_obs_enc[dest_col] = patient_obs_enc[source_col].apply(
                lambda x: None if x is None else x.split(',')[1])
        patient_obs_enc = patient_obs_enc.drop(columns=[col[1] for col in col_map])
        return patient_obs_enc
