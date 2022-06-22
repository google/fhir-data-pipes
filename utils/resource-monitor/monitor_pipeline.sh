#!/bin/bash

###################################################
# Launches and monitors the batch job with pidstat.
###################################################

# This script:
# 1) Finds the pid of the fhir (HAPI) server and runs pidstat on the server
# 2) Runs pidstat on the database
# 3) Starts batch job with the specified input parameters 
# 4) Runs pidstat on the pipeline
# 5) Stop all pidstat jobs once batch job completes

# Example usage:
# sh monitor_pipeline.sh [num processes] [data description] [output parquet path] [output results path]

set -e

#Monitor resource usage for the HAPI server
echo "Starting resource monitoring of server."
server_pid=$(docker top hapi-server | awk '{print $2}' | sed -n '2 p')
pidstat 1 -r -u -d -h -p $server_pid > $4$2/raw/server_stats_$1_proc.txt &
server_monitor_pid=$!

#Monitor resource usage for the postgres database
echo "Starting resource monitoring of database."
pidstat 1 -r -u -d -h -G postgres -l > $4$2/raw/database_stats_$1_proc.txt &
db_monitor_pid=$!

#Launch batch job and monitor resource usage for the pipeline
echo "Starting batch job with $1 parallel proc."
cd .. 
cd ..
java -cp pipelines/batch/target/batch-bundled-0.1.0-SNAPSHOT.jar org.openmrs.analytics.FhirEtl --fhirServerUrl=http://localhost:8098/fhir --fhirServerUserName=hapi --fhirServerPassword=hapi --outputParquetPath=$3 --resourceList=Patient,Encounter,Observation --batchSize=20 --targetParallelism=$1 2>&1 & 
pidstat 1 -r -u -d -h -p $! > $4$2/raw/pipeline_stats_$1_proc.txt

#Stop all pidstat jobs 
echo "Batch Job Complete... Stopping resource monitoring."
kill -9 $server_monitor_pid
kill -9 $db_monitor_pid