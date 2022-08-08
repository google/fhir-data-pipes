# Copyright 2020 Google LLC
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


"""Sample queries for extracting aggregate stats for a set of observations.

Given a list of observation codes and a date range, this sample module shows
different ways for calculating aggregate values of observations grouped by
patient IDs.
"""

import argparse
from typing import Tuple, List

from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, regexp_replace

from pyspark import SparkConf


def valid_date(date_str: str) -> datetime:
  try:
    return datetime.strptime(date_str, '%Y-%m-%d')
  except ValueError:
    raise argparse.ArgumentTypeError('Valid dates have YYYY-MM-DD format!')


def create_args(parser: argparse.ArgumentParser):
  parser.add_argument(
      '--src_dir',
      help='Directory that includes Parquet files for each FHIR resource type',
      required=True,
      type=str
  )
  parser.add_argument(
      '--last_date',
      help='The last date for aggregating data.',
      default=date.today(),
      type=valid_date
  )
  # TODO: Remove the next arguement once this issues is resolved:
  # https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/55
  parser.add_argument(
      '--base_patient_url',
      help='This is the base url to be added to patient IDs, e.g., ' +
           'http://localhost:8099/openmrs/ws/fhir2/R4/Patient/',
      default='',
      type=str
  )
  parser.add_argument(
      '--num_days',
      help='Number of days on which calculate the indicators.',
      default=28,
      type=int
  )
  parser.add_argument(
      '--code_list',
      help='A list of code values for which aggregates are calculated.',
      required=True,
      type=str,
      nargs='+',
      metavar='CODE'
  )


def find_date_range(args: argparse.ArgumentParser) -> Tuple[str, str]:
  end_date_str = args.last_date.strftime('%Y-%m-%d')
  start_date = args.last_date - timedelta(days=args.num_days)
  start_date_str = start_date.strftime('%Y-%m-%d')
  return start_date_str, end_date_str


def create_max_obs_sql_query(
    base_patient_url: str,
    start_date: str,
    end_date: str,
    code_list: List[str]
) -> str:
  # TODO: Figure out how to do `explode` in SQL; also write the following query
  # with Spark's DataFrame API (using join/explode) to compare.
  sql_query = (
      'SELECT replace(patient.id, "' + base_patient_url + '", "") ' +
      ' AS patient_id, patient.name[0].family AS family, ' +
      'observation.code.coding[0].code AS code, ' +
      'COUNT(0) AS num_obs, ' +
      'MAX(observation.effective.dateTime) AS last_obs, ' +
      'MAX(observation.value.quantity.value) AS max_value ' +
      'FROM patient, observation ' +
      'WHERE patient.id = concat(' +
      '"' + base_patient_url + '", ' +
      'observation.subject.PatientId) ' +
      'AND observation.effective.dateTime > "' + start_date + '" ' +
      'AND observation.effective.dateTime < "' + end_date + '" ' +
      'AND observation.code.coding[0].code IN ' +
      '("' + '","'.join(code_list) + '") ' +
      'GROUP BY patient.id, patient.name[0].family, ' +
      'observation.code.coding[0].code'
  )
  return sql_query


def get_max_obs(
    patient: DataFrame,
    observation: DataFrame,
    base_patient_url: str,
    start_date: str,
    end_date: str,
    code_list: List[str]
) -> DataFrame:
  # We can select columns by string column names or column objects.
  obs_filtered = observation.select(
      'subject.patientId', 'effective.dateTime', 'code', 'value').filter(
      observation.effective.dateTime > start_date).filter(
      observation.effective.dateTime < end_date)
  obs_codes = obs_filtered.select(
      obs_filtered.patientId, obs_filtered.dateTime, obs_filtered.value,
      explode(obs_filtered.code.coding).alias('coding'))
  obs_target_codes = obs_codes.filter(obs_codes.coding.code.isin(code_list))
  grouped_obs = obs_target_codes.groupBy(
      ['patientId', 'coding.code']).agg(
      {'dateTime': 'max', 'value.quantity': 'max', '*': 'count'})
  patient_name = patient.select(
      patient.id, patient.name[0].family.alias('family')).withColumn(
      'actual_id', regexp_replace('id', base_patient_url, ''))
  return patient_name.join(grouped_obs,
                           patient_name.actual_id == grouped_obs.patientId)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  create_args(parser)
  args = parser.parse_args()
  start_date, end_date = find_date_range(args)
  print('Source directory: {0}'.format(args.src_dir))
  print('Date range:  {0} - {1}'.format(start_date, end_date))
  print('Codes are: {0}'.format(args.code_list))
  conf = (SparkConf()
          .setMaster('local[20]')
          .setAppName('SampleIndicatorsApp')
          .set('spark.executor.memory', '1g'))
  spark = SparkSession.builder.config(conf=conf).getOrCreate()

  # Loading Parquet files and some sample count queries
  patient = spark.read.parquet(args.src_dir + '/Patient')
  patient.createOrReplaceTempView('patient')
  patient_count = spark.sql('SELECT COUNT(0) FROM patient')
  observation = spark.read.parquet(args.src_dir + '/Observation')
  observation.createOrReplaceTempView('observation')
  observation_count = spark.sql('SELECT COUNT(0) FROM observation')

  sql_query = create_max_obs_sql_query(
      args.base_patient_url, start_date, end_date, args.code_list)
  print('SQL query is: ' + sql_query)
  max_obs_sql = spark.sql(sql_query)
  max_obs = get_max_obs(patient, observation, args.base_patient_url,
                        start_date, end_date, args.code_list)

  # It is better to do all pipeline construction logic before any statement that
  # materializes data-frames like it is done above.
  print('Finding number of patients ...')
  print(patient.count())
  print('Finding number of patients with SQL ...')
  print(patient_count.head())
  print('Finding number of observations ...')
  print(observation.count())
  print('Finding number of observations with SQL ...')
  print(observation_count.head())
  print('Grouping observations for each patient with SQL ...')
  print(max_obs_sql.show())
  print('Grouping observations for each patient ...')
  print(max_obs.show())
