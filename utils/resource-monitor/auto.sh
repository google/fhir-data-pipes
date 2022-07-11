#!/bin/sh

###################################################
# Automates batch job resource graphing.
###################################################

# This script:
# Takes in a lower bound, upper bound and increment for the number of cores, and graphs resource usage for each run.

# Example usage:
# sh auto.sh [data description] [output parquet path] [output results path] [num processes increment] [lower core limit] [upper core limit]

set -e

if [[ "$#" -ne 6 ]]; then
    printf '%s\n' \
        "Error: Illegal number of arguments. " \
        "Example usage: " \
        "sh $0 [data description] [output parquet path] [output results path] [num processes increment] [lower core limit] [upper core limit]"
    exit 1
fi

i=$5
while [[ $i -le $6 ]]
do
    echo "Running job with $i processes"
    python3 graph_pidstat.py --numProc $i --dataDescription $1 --outputParquetPath $2 --outputResultsPath $3
    i=$(($i+$4))
done
