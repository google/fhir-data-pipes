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
# This script is the entry-point to any indicator calculation logic.
set -e  # Make sure that we exit after the very first error.
print_prefix="INDICATOR GENERATION:"
echo "${print_prefix} Transformation STARTED..."
TEMP_OUT=$(mktemp  /dwh/output/${OUTPUT_CSV}.XXXXXXX.csv)
echo "${print_prefix} Output indicators file is: ${TEMP_OUT}"

spark-submit main.py --src_dir=${PARQUET_PATH} --output_csv=${TEMP_OUT} \
  --last_date=${LAST_DATE} --num_days=${NUM_DAYS}

COUNT=$(wc -l < ${TEMP_OUT})
echo "${print_prefix} Number of records generated: ${COUNT}"
# check status
if [[ ${COUNT} -eq 0 ]]; then
  echo "${print_prefix} Zero records generated, please check your indicator definition"
else
  echo "${print_prefix} Completed Successfully!"
fi


