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


"""This is the main higher level library to query FHIR resources.

The public interface of this library is intended to be independent of the actual
query engine, e.g., Spark, SQL/BigQuery, etc. The only exception is a single
function that defines the source of the data.
"""

# See https://stackoverflow.com/questions/33533148 why this is needed.
from __future__ import annotations
from enum import Enum
from typing import List
import pandas
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

import common


class Runner(Enum):
  SPARK = 1
  BIG_QUERY = 2
  #FHIR_SERVER = 3


def patient_query_factory(runner: Runner, data_source: str) -> PatientQuery:
  """Returns the right instance of `PatientQuery` based on `data_source`.

  Args:
    runner: The runner to use for making data queries
    data_source: The definition of the source, e.g., directory containing
      Parquet files or a BigQuery dataset.

  Returns:
    The created instance.

  Raises:
    ValueError: When the input `data_source` is malformed or not implemented.
  """
  if runner == Runner.SPARK:
    return _SparkPatientQuery(data_source)
  if runner == Runner.BIG_QUERY:
    return _BigQueryPatientQuery(data_source)
  raise ValueError('Query engine {} is not supported yet.'.format(runner))


class _ObsConstraints():

  def __init__(self, code: str, values: List[str] = None,
       min_value: float = None, max_value: float = None,
       min_time: str = None, max_time: str = None) -> None:
     self._code = code
     self._values = values
     self._min_time = min_time
     self._max_time = max_time
     self._min_value = min_value
     self._max_value = max_value

  @staticmethod
  def time_constraint(min_time: str = None, max_time: str = None):
    if not min_time and not max_time:
      return 'TRUE'
    cl = []
    if min_time:
      cl.append('dateTime >= "{}"'.format(min_time))
    if max_time:
      cl.append('dateTime <= "{}"'.format(max_time))
    return ' AND '.join(cl)

  def sql(self) -> str:
    """This creates a constraint string with WHERE syntax in SQL.

    All of the observation constraints specified by this instance are joined
    together into an `AND` clause.
    """
    cl = [self.time_constraint(self._min_time, self._max_time)]
    cl.append('coding.code="{}"'.format(self._code))
    if self._values:
      codes_str = ','.join(['"{}"'.format(v) for v in self._values])
      cl.append('value.string IN ({})'.format(codes_str))
    elif self._min_value or self._max_value:
      if self._min_value:
        cl.append(' value.quantity.value >= {} '.format(self._min_value))
      if self._max_value:
        cl.append(' value.quantity.value <= {} '.format(self._max_value))
    return '({})'.format(' AND '.join(cl))


class PatientQuery():
  """The main class for specifying a patient query.

  The expected usage flow is:
  - The user specifies where the data comes from and what query engine should
    be used, e.g., Parquet files with Spark, a SQL engine like BigQuery, or even
    a FHIR server/API (future).
  - Constraints are set, e.g., observation codes, values, date, etc.
  - The query is run on the underlying engine and a Pandas DataFrame is created.
  - The DataFrame is fetched or more manipulation is done on it by the library.
  """

  def __init__(self):
    self._code_constraint = {}
    self._include_all_codes = False
    self._all_codes_min_time = None
    self._all_codes_max_time = None

  def include_obs_in_value_and_time_range(self, code: str,
      min_val: float = None, max_val: float = None, min_time: str = None,
      max_time: str = None) -> PatientQuery:
    if code in self._code_constraint:
      raise ValueError('Duplicate constraints for code {}'.format(code))
    self._code_constraint[code] = _ObsConstraints(
        code, min_value=min_val, max_value=max_val, min_time=min_time, max_time=max_time)
    return self

  def include_obs_values_in_time_range(self, code: str,
      values: List[str] = None, min_time: str = None,
      max_time: str = None) -> PatientQuery:
    if code in self._code_constraint:
      raise ValueError('Duplicate constraints for code {}'.format(code))
    self._code_constraint[code] = _ObsConstraints(
        code, values=values, min_time=min_time, max_time=max_time)
    return self

  def include_all_other_codes(self, include: bool = True, min_time: str = None,
      max_time: str = None) -> PatientQuery:
    self._include_all_codes = True
    self._all_codes_min_time = min_time
    self._all_codes_max_time = max_time
    return self

  def all_constraints_sql(self) -> str:
    if not self._code_constraint:
      if self._include_all_codes:
        return 'TRUE'
      else:
        return 'FALSE'
    constraints_str = ' OR '.join(
        [self._code_constraint[code].sql() for code in self._code_constraint])
    if not self._include_all_codes:
      return '({})'.format(constraints_str)
    others_str = ' AND '.join(
        ['coding.code!="{}"'.format(code) for code in self._code_constraint] + [
            _ObsConstraints.time_constraint(self._all_codes_min_time,
                                            self._all_codes_max_time)])
    return '({} OR ({}))'.format(constraints_str, others_str)

  # TODO remove `base_patient_url` parameter once issue #55 is fixed.
  def find_patient_aggregates(self, base_patient_url: str) -> pandas.DataFrame:
    raise NotImplementedError('This should be implemented by sub-classes!')


class _SparkPatientQuery(PatientQuery):

  def __init__(self, file_root: str):
    super().__init__()
    self._file_root = file_root
    self._spark = None
    self._patient_df = None
    self._obs_df = None
    self._flat_obs = None
    self._patient_agg_obs_df = None

  def find_patient_aggregates(self, base_patient_url: str) -> pandas.DataFrame:
    if not self._spark:
      conf = (SparkConf()
              .setMaster('local[20]')
              .setAppName('IndicatorsApp')
              .set('spark.driver.memory', '10g')
              .set('spark.executor.memory', '2g')
              # See: https://spark.apache.org/docs/latest/security.html
              .set('spark.authenticate', 'true')
              )
      self._spark = SparkSession.builder.config(conf=conf).getOrCreate()
      # Loading Parquet files and flattening only happens once.
      self._patient_df = self._spark.read.parquet(self._file_root + '/Patient')
      self._obs_df = self._spark.read.parquet(self._file_root + '/Observation')
      # TODO create inspection functions
      common.custom_log(
          'Number of Patient resources= {}'.format(self._patient_df.count()))
      common.custom_log(
          'Number of Observation resources= {}'.format(self._obs_df.count()))
      self._flat_obs = _SparkPatientQuery._flatten_obs(self._obs_df)
      common.custom_log(
          'Number of flattened obs rows = {}'.format(self._flat_obs.count()))
    work_df = self._flat_obs.where(self.all_constraints_sql())
    agg_obs_df = _SparkPatientQuery._aggregate_all_codes_per_patient(work_df)
    common.custom_log(
      'Number of aggregated obs= {}'.format(agg_obs_df.count()))
    self._patient_agg_obs_df = _SparkPatientQuery.join_patients_agg_obs(
        self._patient_df, agg_obs_df, base_patient_url)
    common.custom_log('Number of joined patient_agg_obs= {}'.format(
        self._patient_agg_obs_df.count()))
    # Spark is supposed to automatically cache DFs after shuffle but it seems
    # this is not happening!
    self._patient_agg_obs_df.cache()
    return self._patient_agg_obs_df.toPandas()

  @staticmethod
  def _flatten_obs(obs: DataFrame) -> DataFrame:
    """Creates a flat version of Observation FHIR resources.

    Args:
      obs: A collection of Observation FHIR resources.
    Returns:
      A DataFrame with three columns where `coding` arrays are flattened.
    """
    return obs.select(
        obs.subject.patientId.alias('patientId'),
        obs.effective.dateTime.alias('dateTime'),
        obs.value,
        F.explode(obs.code.coding).alias('coding'))

  @staticmethod
  def _aggregate_all_codes_per_patient(flat_obs: DataFrame,
      codes: List[str]=None ) -> DataFrame:
    """ For each patientId, generates aggregate values for all their observations.

    Args:
        flat_obs: A collection of flattened Observations.
        codes: A list of codes for which aggregates are generated or None to
          indicate all codes.
    Returns:
      A DataFrame with one row for each patient and several aggregate columns
      for each Observation code.
    """
    return flat_obs.groupBy([
        flat_obs.patientId,
    ]).pivot('coding.code', values=codes).agg(
        F.count('*').alias('num_obs'),
        F.min(flat_obs.value.quantity.value).alias('min_value'),
        F.max(flat_obs.value.quantity.value).alias('max_value'),
        F.min(flat_obs.dateTime).alias('min_date'),
        F.max(flat_obs.dateTime).alias('max_date')
    )

  @staticmethod
  def join_patients_agg_obs(
      patients: DataFrame,
      agg_obs: DataFrame,
      base_patient_url: str) -> DataFrame:
    """Joins a collection of Patient FHIR resources with an aggregated obs set.

    Args:
      patients: A collection of Patient FHIR resources.
      agg_obs: Aggregated observations from `aggregate_all_codes_per_patient()`.
    Returns:
      Same `agg_obs` with corresponding patient information joined.
    """
    flat_patients = patients.select(
        patients.id, patients.birthDate, patients.gender).withColumn(
        'actual_id', F.regexp_replace('id', base_patient_url, ''))
    return flat_patients.join(
        agg_obs, flat_patients.actual_id == agg_obs.patientId)


class _BigQueryPatientQuery(PatientQuery):
  # TODO implement this!

  def __init__(self, bq_dataset: str):
    super().__init__()
    raise ValueError('BigQuery query engine is not implemented yet!')

