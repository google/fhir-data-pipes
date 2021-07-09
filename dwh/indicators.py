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

import indicator_lib
import query_lib


_CODE_SYSTEM = 'http://snomed.info/sct'
_VL_CODE = '50373000'  # Height
_TB_CODE = '159394AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'  # Diagnosis certainty


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
  # TODO: Remove the next arguement once issues #55 is resolved.
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


def find_date_range(args: argparse.Namespace) -> Tuple[str, str, str]:
  end_date_str = args.last_date.strftime('%Y-%m-%d')
  start_date = args.last_date - timedelta(days=args.num_days)
  start_date_str = start_date.strftime('%Y-%m-%d')
  previous_period_start = args.last_date - timedelta(days=2 * args.num_days)
  previous_period_start_str = previous_period_start.strftime('%Y-%m-%d')
  return start_date_str, end_date_str, previous_period_start_str


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  create_args(parser)
  args = parser.parse_args()
  start_date, end_date, prev_start = find_date_range(args)
  print('Source directory: {0}'.format(args.src_dir))
  print('Date range:  {0} - {1}'.format(start_date, end_date))
  patient_query = query_lib.patient_query_factory(
      query_lib.Runner.SPARK, args.src_dir, _CODE_SYSTEM)
  # TODO check why without this constraint, `validate_indicators.sh` fails.
  patient_query.include_obs_values_in_time_range(
      _VL_CODE, min_time=start_date, max_time=end_date)
  # TODO add more interesting constraints/indicators like:
  #patient_query.include_obs_values_in_time_range(
  #    _TB_CODE, ['159393AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
  #    min_time=prev_start, max_time=start_date)
  patient_query.include_all_other_codes(min_time=start_date, max_time=end_date)
  patient_agg_obs_df = patient_query.find_patient_aggregates(
      args.base_patient_url)
  VL_df_P = indicator_lib.calc_TX_PVLS(
      patient_agg_obs_df, VL_code=_VL_CODE,
      failure_threshold=150, end_date_str=end_date)
  # TX_NEW
  TX_NEW_df_P = indicator_lib.calc_TX_NEW(
      patient_agg_obs_df, ARV_plan='159394AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
      start_drug=['159393AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'], end_date_str=end_date)

  VL_df_P.merge(TX_NEW_df_P, how='outer', left_on=['buckets', 'sup_VL'],
                right_on=['buckets', 'TX_NEW'])\
      .to_csv(args.output_csv, index=False)

