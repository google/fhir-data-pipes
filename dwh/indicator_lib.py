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


"""Set of functions to work with Spark DataFrames containing FHIR resources.

See test_spark.ipynb for real examples of how to create/use these functions.
"""

from typing import List
from datetime import datetime
import pandas
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

def custom_log(t: str) -> None:
  print('[INDICATORS_LOG {}] {}'.format(
    datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), t))

def flatten_obs(obs: DataFrame) -> DataFrame:
  """Creates a flat version of Observation FHIR resources.

  Args:
    obs: A collection of Observation FHIR resources.
  Returns:
    A DataFrame with three columns where `coding` arrays are flattened.
  """
  return obs\
      .withColumn('kv', F.arrays_zip('code.coding','value.codeableConcept.coding'))\
      .withColumn('kv', F.explode('kv'))\
      .select(
          F.col('value'),
          F.col('subject.patientId').alias('patientId'),
          F.col('effective.dateTime').alias('dateTime'),
          F.col('kv.0').alias('coding'),
          F.col('kv.1').alias('valueConcept')
      )


def aggregate_all_codes_per_patient(
    obs: DataFrame,
    codes: List[str]=None,
    start_date: str=None,
    end_date: str=None) -> DataFrame:
  """ For each patientId, generates aggregate values for all their observations.

  Args:
    obs: A collection of Observation FHIR resources.
    codes: A list of codes for which aggregates are generated or None to
      indicate all codes.
    start_date: The first date before which Observations are dropped.
    end_date: The last date after which Observations are dropped.
  Returns:
    A DataFrame with one row for each patient and several aggregate columns for
    each Observation code.
  """
  flat_obs = flatten_obs(obs)
  start_obs = flat_obs
  if start_date:
    start_obs = flat_obs.filter(flat_obs.dateTime > start_date)
  date_obs = start_obs
  if end_date:
    date_obs = start_obs.filter(start_obs.dateTime < end_date)
  return date_obs.groupBy([
      flat_obs.patientId,
  ]).pivot('coding.code', values=codes).agg(
      F.count('*').alias('num_obs'),
      F.min(flat_obs.value.quantity.value).alias('min_value'),
      F.max(flat_obs.value.quantity.value).alias('max_value'),
      F.min(flat_obs.dateTime).alias('min_date'),
      F.max(flat_obs.dateTime).alias('max_date'),
      F.first(flat_obs.valueConcept.code, True).alias('first_coded_value'),
      F.last(flat_obs.valueConcept.code, True).alias('last_coded_value'),
      F.first(flat_obs.valueConcept.display, True).alias('first_coded_value_display'),
      F.last(flat_obs.valueConcept.display, True).alias('last_coded_value_display')
  )


def join_patients_agg_obs(
    patients: DataFrame,
    agg_obs: DataFrame,
    base_patient_url: str):
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


def find_age_band(birth_date: str, end_date: datetime) -> str:
  """Given the birth date, finds the age_band for PEPFAR disaggregation."""
  age = None
  try:
    # TODO handle all different formats https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/174
    birth = datetime.strptime(birth_date, '%Y-%m-%d')
    age = int((end_date - birth).days / 365.25)
  except Exception as e:
    custom_log('Invalid birth_date format: {}'.format(e))
    age = 999999

  if age == 999999:
    return 'ERROR'
  if age < 1:
    return '0-1'
  if age <= 4:
    return '1-4'
  if age <= 9:
    return '5-9'
  if age <= 14:
    return '10-14'
  if age <= 19:
    return '15-19'
  if age <= 24:
    return '20-24'
  if age <= 49:
    return '25-49'
  return '50+'


def agg_buckets(birth_date: str, gender: str, end_date: datetime) -> List[str]:
  """Generates the list of all PEPFRA disaggregation buckets."""
  age_band = find_age_band(birth_date, end_date)
  return [age_band + '_' + gender, 'ALL-AGES_' + gender,
          age_band + '_ALL-GENDERS', 'ALL-AGES_ALL-GENDERS']


def calc_TX_PVLS(patient_agg_obs: DataFrame, VL_code: str, failure_threshold: int,
    end_date_str: str = None) -> pandas.DataFrame:
  """Calculates TX_PVLS indicator with its corresponding disaggregations.

  Args:
    patient_agg_obs: A DataFrame generated by `join_patients_agg_obs()`.
    VL_code: The code for viral load values.
    failure_threshold: VL count threshold of failure.
    end_date: The string representation of the last date as 'YYYY-MM-DD'.
  Returns:
  """
  end_date = datetime.today()
  if end_date_str:
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
  agg_buckets_udf = F.UserDefinedFunction(
      lambda a, g: agg_buckets(a, g, end_date),
      T.ArrayType(T.StringType()))
  VL_df = patient_agg_obs.withColumn(
      'sup_VL', patient_agg_obs[VL_code + '_max_value'] < failure_threshold).withColumn(
      'agg_buckets', agg_buckets_udf(
          patient_agg_obs['birthDate'], patient_agg_obs['gender'])
  )
  num_patients = VL_df.count()
  VL_agg_P = VL_df.select(
      VL_df.sup_VL,
      F.explode(VL_df.agg_buckets).alias('agg_bucket')).groupBy(
      'sup_VL', 'agg_bucket').agg(
      F.count('*').alias('count')).toPandas().sort_values(
      ['agg_bucket', 'sup_VL'])
  VL_agg_P['ratio'] = VL_agg_P['count']/num_patients
  return VL_agg_P


def calc_TX_NEW(patient_agg_obs: DataFrame, ARV_plan: str, start_drug: str,
    end_date_str: str = None) -> pandas.DataFrame:
  """Calculates TX_NEW indicator with its corresponding disaggregations.

  TX_NEW indicator counts the number of adults and children newly enrolled
  on antiretroviral therapy (ART) within the provided

  Args:
    patient_agg_obs: A DataFrame generated by `join_patients_agg_obs()`.
    ARV_plan: The concept question code for ANTIRETROVIRAL PLAN
    start_drug: The concept answer code for START DRUG
    end_date: The string representation of the last date as 'YYYY-MM-DD'.
  Returns:
  """
  end_date = datetime.today()
  if end_date_str:
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
  agg_buckets_udf = F.UserDefinedFunction(
      lambda a, g: agg_buckets(a, g, end_date),
      T.ArrayType(T.StringType()))
  TX_NEW_df = patient_agg_obs \
      .withColumn('TX_NEW', F.when(F.col(ARV_plan + '_first_coded_value')
                                   .isin(start_drug), True).otherwise(False)) \
      .withColumn(
      'agg_buckets', agg_buckets_udf(
          patient_agg_obs['birthDate'], patient_agg_obs['gender'])
  )
  num_patients = TX_NEW_df.count()
  aggregates_P = TX_NEW_df.select(
      TX_NEW_df.TX_NEW,
      F.explode(TX_NEW_df.agg_buckets).alias('agg_bucket')).groupBy(
      'TX_NEW', 'agg_bucket').agg(
      F.count('*').alias('count')).toPandas()\
      .sort_values(['agg_bucket', 'TX_NEW'])
  aggregates_P['ratio'] = aggregates_P['count']/num_patients
  return aggregates_P

