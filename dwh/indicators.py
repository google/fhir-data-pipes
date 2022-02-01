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
from dateutil import parser as date_parser

import indicator_lib
import query_lib


_CODE_SYSTEM='http://www.ampathkenya.org'

# For information about the following codes see:
# https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/179#issuecomment-895040775
# and also `synthea` models in this repo.

# Question codes:
_VL_CODE = '856'  # HIV VIRAL LOAD
_ARV_PLAN = '1255'  # ANTIRETROVIRAL PLAN
_TX_TB_PLAN = '1268'  # TUBERCULOSIS TREATMENT PLAN
# TODO: Add TB prevention plans in the synthetic data; seems currently missing.
_TB_PREV_plan = '1268'  # TUBERCULOSIS TREATMENT PLAN
_TB_screening = '6174'  # REVIEW OF TUBERCULOSIS SCREENING QUESTIONS

# Answer codes:
_YES_CODE = '1065'
_CONTINUE_REGIMEN = '1257'  # CONTINUE REGIMEN
_START_DRUGS = '1256'  # START DRUGS
_COMPLETE_REGIMEN = '1260'  # STOP ALL MEDICATIONS
_STOP_ALL_MED = '1260'  # STOP ALL MEDICATIONS


def valid_date(date_str: str) -> datetime:
  try:
    return date_parser.parse(date_str)
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
           'http://localhost:8099/openmrs/ws/fhir2/R4/',
      default='http://localhost:8099/openmrs/ws/fhir2/R4/',
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


def find_date_range(args: argparse.Namespace) -> Tuple[str, str, str, str, str]:
  end_date_str = args.last_date.strftime('%Y-%m-%d')
  start_date = args.last_date - timedelta(days=args.num_days)
  start_date_str = start_date.strftime('%Y-%m-%d')
  previous_period_start = args.last_date - timedelta(days=2 * args.num_days)
  previous_period_start_str = previous_period_start.strftime('%Y-%m-%d')
  semiannual_start = args.last_date - timedelta(days=6 * args.num_days)
  semiannual_start_str = semiannual_start.strftime('%Y-%m-%d')
  quarterly_start = args.last_date - timedelta(days=3 * args.num_days)
  quarterly_start_str = quarterly_start.strftime('%Y-%m-%d')
  return (start_date_str, end_date_str, previous_period_start_str,
         semiannual_start_str, quarterly_start_str)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  create_args(parser)
  args = parser.parse_args()
  (start_date, end_date, prev_start, semiannual_start_str,
      quarterly_start_str) = find_date_range(args)
  print('Source directory: {0}'.format(args.src_dir))
  print('Date range:  {0} - {1}'.format(start_date, end_date))
  # TODO check why without this constraint, `validate_indicators.sh` fails.
  # Monthly query
  monthly_query = query_lib.patient_query_factory(
      query_lib.Runner.SPARK, args.src_dir, _CODE_SYSTEM).include_obs_values_in_time_range(
      _VL_CODE, min_time=start_date, max_time=end_date)
  monthly_query.include_all_other_codes(min_time=start_date, max_time=end_date)
  # Semiannual query
  semi_annual_query = query_lib.patient_query_factory(
      query_lib.Runner.SPARK, args.src_dir, _CODE_SYSTEM).include_all_other_codes(
      min_time=semiannual_start_str, max_time=end_date)
  # Prev Month query
  prev_month_query = query_lib.patient_query_factory(
      query_lib.Runner.SPARK, args.src_dir, _CODE_SYSTEM).include_all_other_codes(
      min_time=prev_start, max_time=end_date)
  # Quarterlly query
  quarterly_query = query_lib.patient_query_factory(
      query_lib.Runner.SPARK, args.src_dir, _CODE_SYSTEM).include_all_other_codes(
      min_time=quarterly_start_str, max_time=end_date)

  # Fetch aggregated obs
  current_month_df = monthly_query.get_patient_obs_view(args.base_patient_url)
  prev_month_df = prev_month_query.get_patient_obs_view(args.base_patient_url)
  annual_df = semi_annual_query.get_patient_obs_view(args.base_patient_url)
  quarterly_df = quarterly_query.get_patient_obs_view(args.base_patient_url)

  VL_df = indicator_lib.calc_TX_PVLS(
      current_month_df, VL_code=_VL_CODE,
      failure_threshold=10000, end_date_str=end_date)
  # TX_NEW
  TX_NEW_df = indicator_lib.calc_TX_NEW(
      current_month_df, ARV_plan=_ARV_PLAN,
      start_drug=[_START_DRUGS], end_date_str=end_date)

  TB_STAT_df = indicator_lib.calc_TB_STAT(
      current_month_df, TB_TX_plan=_TX_TB_PLAN, ARV_plan=_ARV_PLAN,
      TB_plan_answer=[_START_DRUGS], end_date_str=end_date)

  TX_CURR_df = indicator_lib.calc_TX_CURR(
      current_month_df, ARV_plan=_ARV_PLAN,
      ARV_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      end_date_str=end_date)

  TB_ART_df = indicator_lib.calc_TB_ART(
      current_month_df, TB_TX_plan=_TX_TB_PLAN, ARV_plan=_ARV_PLAN,
      TB_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      ARV_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      end_date_str=end_date)

  TB_PREV_df = indicator_lib.calc_TB_PREV(
      prev_month_df, TB_PREV_plan=_TB_PREV_plan, ARV_plan=_ARV_PLAN,
      TB_PREV_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      TB_CURR_plan_answer=[_COMPLETE_REGIMEN],
      ART_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      end_date_str=end_date)

  TX_TB_df = indicator_lib.calc_TX_TB(
      annual_df, TX_TB_plan=_TX_TB_PLAN, ARV_plan=_ARV_PLAN,
      TB_screening=_TB_screening, YES_CODE=_YES_CODE,
      TX_TB_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      ART_plan_answer=[_CONTINUE_REGIMEN, _START_DRUGS],
      end_date_str=end_date)

  # TODO the logic behind this merge is not clear, especially for null keys.
  VL_df.merge(TX_NEW_df, how='outer', left_on=['buckets', 'sup_VL'],
                right_on=['buckets', 'TX_NEW']).merge(
      TB_STAT_df, how='outer', left_on=['buckets', 'sup_VL'],
      right_on=['buckets', 'TB_STAT']).merge(
      TX_CURR_df, how='outer', left_on=['buckets', 'sup_VL'],
      right_on=['buckets', 'TX_CURR']).merge(
      TB_ART_df, how='outer', left_on=['buckets', 'sup_VL'],
      right_on=['buckets', 'TB_ART']).merge(
      TB_PREV_df, how='outer', left_on=['buckets', 'sup_VL'],
      right_on=['buckets', 'TB_PREV']).merge(
      TX_TB_df, how='outer', left_on=['buckets', 'sup_VL'],
      right_on=['buckets', 'TX_TB']
  ).to_csv(args.output_csv, index=False)
