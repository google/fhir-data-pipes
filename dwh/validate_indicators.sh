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
  echo "ERROR: 'virtualnv' not found; make sure it is installed and is in PATH."
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
python -m unittest query_lib_test.PatientQueryTest

# Run indicator calculation logic.

TEMP_OUT=$(mktemp indicators_output_XXXXXX.csv --tmpdir)
echo "Output indicators file is: ${TEMP_OUT}"

spark-submit indicators.py --src_dir=./test_files \
  --last_date=2020-12-30 --num_days=28 --output_csv=${TEMP_OUT}

##########################################
# Assertion function that tests aggregates generated
# by comparing them to what is expected
# Arguments:
#   $expected_aggr which is the expected aggregates
#   $indicator_label which is the indicator label
#   $col_index which is the column index for indicator value
##########################################
function assert() {
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
    exit 1
  fi
}

# PVLS counts
assert "The number of suppressed vs non-suppressed vs none are" "34,13,0,5" 3
# PVLS ratio
assert "The ratio of suppressed vs non-suppressed vs none are" "0.723,0.277,0,0.106" 4
# TX_NEW counts
assert "The number of TX_NEW vs non-TX_NEW vs none are" "28,31,0,5" 6
# TX_NEW ratio
assert "The ratio of TX_NEW vs non-TX_NEW vs none are" "0.475,0.525,0,0.0847" 7

echo "SUCCESS!"
deactivate
