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


"""The main binary to calculate PEPFAR indicators.

"""

import argparse
from typing import Tuple
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark import SparkConf

import indicator_lib


_CODE_LIST = ['5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',  # Weight
              '5090AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',  # Height
              ]


def custom_log(t: str) -> None:
  print('[INDICATORS_LOG {}] {}'.format(
    datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), t))


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
      default='http://localhost:8099/openmrs/ws/fhir2/R4/Patient/',
      type=str
  )
  parser.add_argument(
      '--num_days',
      help='Number of days on which calculate the indicators.',
      default=28,
      type=int
  )
  parser.add_argument(
      '--output_csv',
      help='The output CSV file',
      required=True,
      type=str
  )


def find_date_range(args: argparse.ArgumentParser) -> Tuple[str, str]:
  end_date_str = args.last_date.strftime('%Y-%m-%d')
  start_date = args.last_date - timedelta(days=args.num_days)
  start_date_str = start_date.strftime('%Y-%m-%d')
  return start_date_str, end_date_str


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  create_args(parser)
  args = parser.parse_args()
  start_date, end_date = find_date_range(args)
  print('Source directory: {0}'.format(args.src_dir))
  print('Date range:  {0} - {1}'.format(start_date, end_date))
  conf = (SparkConf()
          .setMaster('local[20]')
          .setAppName('IndicatorsApp')
          .set('spark.driver.memory', '10g')
          .set('spark.executor.memory', '2g')
          # See: https://spark.apache.org/docs/latest/security.html
          .set('spark.authenticate', 'true')
          )
  spark = SparkSession.builder.config(conf=conf).getOrCreate()
  # Loading Parquet files and do some count queries for sanity checking.
  patient_df = spark.read.parquet(args.src_dir + '/Patient')
  custom_log('Number of Patient resources= {}'.format(patient_df.count()))
  observation_df = spark.read.parquet(args.src_dir + '/Observation')
  custom_log(
    'Number of Observation resources= {}'.format(observation_df.count()))
  agg_obs_df = indicator_lib.aggregate_all_codes_per_patient(
      observation_df, _CODE_LIST,  start_date, end_date)
  custom_log('Number of aggregated obs= {}'.format(agg_obs_df.count()))
  patient_agg_obs_df = indicator_lib.join_patients_agg_obs(
      patient_df, agg_obs_df, args.base_patient_url)
  custom_log(
    'Number of joined patient_agg_obs= {}'.format(patient_agg_obs_df.count()))

  # Spark is supposed to automatically cache DFs after shuffle but it seems
  # this is not happening!
  patient_agg_obs_df.cache()
  VL_df_P = indicator_lib.calc_TX_PVLS(
      patient_agg_obs_df, VL_code='5090AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
      end_date_str=end_date)
  VL_df_P.to_csv(args.output_csv, index=False)

