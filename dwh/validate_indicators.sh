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

# Run indicator calculation logic.

TEMP_OUT=$(mktemp indicators_output_XXXXXX.csv --tmpdir)
echo "Output indicators file is: ${TEMP_OUT}"

spark-submit indicators.py --src_dir=./test_files \
  --last_date=2020-12-30 --num_days=28 --output_csv=${TEMP_OUT}

counts=$(cat ${TEMP_OUT} | awk -F, '
    BEGIN {num_true = 0; num_false = 0; num_none = 0;}
    /False,ALL-AGES_ALL-GENDERS/ {num_false=$3}
    /True,ALL-AGES_ALL-GENDERS/ {num_true=$3}
    /None,ALL-AGES_ALL-GENDERS/ {num_none=$3}
    END {printf("%d,%d,%d", num_true, num_false, num_none); }')

echo "Number of suppressed vs non-suppressed vs none: ${counts}"
if [[ "${counts}" != "34,13,0" ]]; then
  echo "ERROR: The number of  suppressed vs non-suppressed vs none are " \
    "expected to be '34,13,0' GOT ${counts}"
  exit 1
fi
echo "SUCCESS!"