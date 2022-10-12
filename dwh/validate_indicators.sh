#!/bin/bash
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
#
# This script:
# 1) Creates a virtualenv at `venv_test`
# 2) Installs python package requirements.
# 3) Runs the indicator calculation logic through PySpark.
# 4) Verifies the generated output.

set -e  # Make sure that we exit after the very first error.

# Set up Spark and requirements

declare -r has_virtualenv=$(which virtualenv)
if [[ -z ${has_virtualenv} ]]; then
  echo "ERROR: 'virtualenv' not found; make sure it is installed and is in PATH."
  exit 1
fi
virtualenv -p python3 venv_test
source ./venv_test/bin/activate
# Making sure the pip3 is from the virtualenv
declare -r has_pip3=$(which pip3 | grep 'venv_test')
if [[ -z ${has_pip3} ]]; then
  echo "ERROR: could not find pip3 in 'venv_test/' ${has_pip3}!"
  exit 1
fi
pip3 install -r requirements.txt

# Run unit-tests first:
python -m unittest query_lib_spark_test.SparkPatientQueryTest \
  query_lib_compatibility_tests.PatientQueryTestSpark \
  query_lib_compatibility_tests.PatientQueryTestBigQuery


# Run indicator calculation logic.

TEMP_OUT=$(mktemp indicators_output_XXXXXX.csv --tmpdir)
echo "Output indicators file is: ${TEMP_OUT}"

# Setting the reporting period to a year because the synthetic data is sparse.
spark-submit indicators.py --src_dir=./test_files/parquet_big_db_r4 \
  --last_date=2010-01-01 --num_days=365 --output_csv=${TEMP_OUT}

##########################################
# Assertion function that tests aggregates generated
# by comparing them to what is expected
# Arguments:
#   $1 a message identifying the indicator being validated
#   $2 the expected aggregates as a string containing comma separated values
#   $3 the column index in the CSV for the indicator value
# Globals:
#   ${FAILED} is set if the actual value is different from the expected
##########################################
function validate() {
  local indicator_label=$1
  local expected=$2
  local col_index=$3
  actual=$(cat ${TEMP_OUT} | awk -v col_index="$col_index" -F, '
      BEGIN {value_true = 0; value_false = 0; value_none = 0;}
      /False,ALL-AGES_ALL-GENDERS/ {value_false=$col_index}
      /True,ALL-AGES_ALL-GENDERS/ {value_true=$col_index}
      /None,ALL-AGES_ALL-GENDERS/ {value_none=$col_index}
      /True,25-49_male/ {value_male_25=$col_index}
      END {printf("%.3g,%.3g,%.3g,%.3g", value_true, value_false, value_none, value_male_25); }')

  echo "${indicator_label} : ${actual}"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "ERROR: ${indicator_label}" \
      "expected to be ${expected} GOT ${actual}"
    FAILED="yes"
  fi
}

FAILED=""
# PVLS counts
validate "Suppressed, non-suppressed, none, male_25 numbers are" "8,752,0,2" 3
# PVLS ratio
validate "Suppressed, non-suppressed, none, male_25 ratios are" \
  "0.0105,0.989,0,0.00263" 4
# TX_NEW counts
# TODO validate these manually by querying the DB
validate "TX_NEW, non-TX_NEW, none, male_25 numbers are" "95,439,0,24" 6
# TX_NEW ratio
validate "TX_NEW, non-TX_NEW, none, male_25 ratios are" "0.178,0.822,0,0.0449" 7
# TB_STAT counts
validate "TB_STAT, non-TB_STAT, none, male_25 numbers are" "86,448,0,29" 9
# TB_STAT ratio
validate "TB_STAT, non-TB_STAT, none, male_25 ratios are" \
  "0.161,0.839,0,0.0543" 10
# TX_CURR counts
validate "TX_CURR, non-TX_CURR, none, male_25 numbers are" "116,418,0,29" 12
# TX_CURR ratio
validate "TX_CURR, non-TX_CURR, none, male_25 ratios are" \
  "0.217,0.783,0,0.0543" 13
# TB_ART counts
validate "TB_ART, non-TB_ART, none, male_25 numbers are" "43,491,0,13" 15
# TB_ART ratio
validate "TB_ART, non-TB_ART, none, male_25 ratios are" \
  "0.0805,0.919,0,0.0243" 16
# TB_PREV counts
validate "TB_PREV, non-TB_PREV, none, male_25 numbers are" "57,6.35e+03,0,8" 18
# TB_PREV ratio
validate "TB_PREV, non-TB_PREV, none, male_25 ratios are" \
  "0.0089,0.991,0,0.00125" 19

# TX_TB counts
validate "TX_TB, non-TX_TB, none, male_25 numbers are" "0,6.4e+03,0,0" 21
# TX_TB ratio
validate "TX_TB, non-TX_TB, none, male_25 ratios are" "0,1,0,0" 22

if [[ -n "${FAILED}" ]]; then
  echo "FAILED!"
else
  echo "SUCCESS!"
fi
deactivate
