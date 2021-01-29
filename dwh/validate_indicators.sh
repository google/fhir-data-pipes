#!/bin/bash

TEMP_OUT=$(mktemp indicators_output_XXXXXX.csv --tmpdir)
#TEMP_OUT="/usr/local/google/home/bashir/temp/tmp/TX_PVLS.csv"
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
  echo "ERROR! The number of  suppressed vs non-suppressed vs none are expected to be '34,13,0' GOT ${counts}"
  exit 1
fi
echo "SUCCESS!"