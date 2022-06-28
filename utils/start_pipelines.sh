#!/bin/bash
# Copyright 2020 Google LLC
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

# This is a tool to start the streaming and batch (optional) pipelines in a way
# that they are in sync, i.e., each resource is picked by one or the other.

# Exit on first error and treat access to unset variables as error.
set -e -u

#######################################
# Prints usage information and exits.
#######################################
function usage() {
  echo "A tool for synchronized run of streaming and batch pipelines."
  echo "Usage: ${0} [options]"
  echo "Options:"
  echo "-batchJar JAR_FILE, the bundled jar file for the batch pipeline."
  echo "  default: ./batch/target/batch-bundled[*].jar"
  echo ""
  echo "-batchLog FILE, the file to capture batch pipeline logs."
  echo "  default: tmp/streaming.log"
  echo ""
  echo "-c|-config CONFIG_FILE, the file containing database config including FHIR mapping."
  echo "  default: ./utils/dbz_event_to_fhir_config.json"
  echo ""
  echo "-b|-enableBatch, run the batch pipeline too."
  echo "  default: The batch pipeline is not run by default."
  echo ""
  echo "-o|-outputDir OUTPUT_DIR, the output directory for Parquet files."
  echo "  default: None; this is a required option."
  echo ""
  echo "-periodStart YYYY-MM-DDThh:mm:ss the batch pipeline only fetches data"
  echo "  for patients in this time period; this has no impact on the streaming pipeline."
  echo "  The time part (after 'T') is not mandatory and is zero by default."
  echo "  example: 2021-04-22T10:00:00"
  echo "  default: No active period, i.e., all resources are fetched."
  echo ""
  echo "-secondsToFlushStreaming SECONDS, number of seconds after which the resources fetched by "
  echo "  the streaming pipeline are flushed to a Parquet file; default: 3600"
  echo ""
  echo "-secondsToFlushBatch SECONDS, same value for the batch pipeline. Note this should "
  echo "  generally be smaller than the streaming mode; default 600."
  echo ""
  echo "-s|-skipConfirmation, proceed to run the pipelines without asking for confirmation."
  echo "  default: does not skip, i.e., it asks for explicit confirmation."
  echo ""
  echo "-u|-sourceUrl URL, the URL for the source FHIR store."
  echo "  default: http://localhost:8099/openmrs/ws/fhir2/R4"
  echo ""
  echo "-streamingLog FILE, the file to capture streaming pipeline logs."
  echo "  default: tmp/streaming.log"
  echo ""
  echo "-streamingJar JAR_FILE, the bundled jar file for the streaming pipeline."
  echo "  default: ./streaming/target/streaming-bundled*.jar."
  # TODO: The above default does not seem to be right; fix and test!
  echo ""
  # TODO: Add the feature for passing extra options directly to the pipelines.
  # echo "If there are any other options remaining at the end (extra-options), they are passed to "
  # echo "any pipelines that are run."
  # echo ""
  exit 1
}

#######################################
# Checks that at least one argument is still available on the command line options.
# Globals:
#   None
# Arguments:
#   The first argument is the name of the option just processed; the rest is what
#   is left from processing options.
#######################################
function check_next_arg() {
  if [[ $# -le 1 ]]; then
    echo "ERROR: Expected option after ${1}"
    exit 1
  fi
}

#######################################
# Prints usage information and exits.
# Globals:
#   STREAMING_JAR the jar file to be used for the streaming pipeline
#   BATCH_JAR the jar file to be used for the batch pipeline
#   OUTPUT_DIR the output directory for storing Parquet files
#   SOURCE_URL the URL of the FHIR server
#   CONFIG_FILE the database config file
#   ENABLE_BATCH set to non-empty if batch pipeline should be run
#   FLUSH_STREAMING number of seconds to flush streaming output
#   FLUSH_BATCH number of seconds to flush batch output
#   STREAMING_LOG the log file for the streaming pipeline
#   BATCH_LOG the log file for the batch pipeline
#   SKIP_CONF non-empty if the confirmation step should be skipped
#   SINK_PATH the FHIR Server URL
#   SINK_USERNAME the FHIR Server username
#   SINK_PASSWORD the FHIR Server password
# Arguments:
#   The command line arguments passed to the main script.
#######################################
function process_options() {
  OUTPUT_DIR=""
  STREAMING_JAR=""
  BATCH_JAR=""
  SOURCE_URL="http://localhost:8099/openmrs/ws/fhir2/R4"
  CONFIG_FILE="../utils/dbz_event_to_fhir_config.json"
  FLUSH_STREAMING=3600
  FLUSH_BATCH=600
  ENABLE_BATCH=""
  STREAMING_LOG="tmp/streaming.log"
  BATCH_LOG="tmp/batch.log"
  SKIP_CONF=""
  PERIOD_START=""
  SINK_PATH=""
  SINK_USERNAME=""
  SINK_PASSWORD=""

  while [[ $# -gt 0 ]]; do
    local opt="$1"
    shift
    case ${opt} in
      -o|-outputDir)
        check_next_arg "${opt}" "$@"
        OUTPUT_DIR=$1
        shift
        ;;

      -streamingJar)
        check_next_arg "${opt}" "$@"
        STREAMING_JAR=$1
        shift
        ;;

      -batchJar)
        check_next_arg "${opt}" "$@"
        BATCH_JAR=$1
        shift
        ;;

      -b|-enableBatch)
        ENABLE_BATCH="enabled"
        ;;

      -u|-sourceUrl)
        check_next_arg "${opt}" "$@"
        SOURCE_URL=$1
        shift
        ;;

      -c|-config)
        check_next_arg "${opt}" "$@"
        CONFIG_FILE=$1
        shift
        ;;

      -secondsToFlushStreaming)
        check_next_arg "${opt}" "$@"
        FLUSH_STREAMING=$1
        shift
        ;;

      -secondsToFlushBatch)
        check_next_arg "${opt}" "$@"
        FLUSH_BATCH=$1
        shift
        ;;

      -periodStart)
        check_next_arg "${opt}" "$@"
        PERIOD_START=$1
        shift
        ;;

      -streamingLog)
        check_next_arg "${opt}" "$@"
        STREAMING_LOG=$1
        shift
        ;;

      -batchLog)
        check_next_arg "${opt}" "$@"
        BATCH_LOG=$1
        shift
        ;;
      
      -fhirSinkPath)
        check_next_arg "${opt}" "$@"
        SINK_PATH=$1
        shift
        ;;

      -sinkUsername)
        check_next_arg "${opt}" "$@"
        SINK_USERNAME=$1
        shift
        ;;

      -sinkPassword)
        check_next_arg "${opt}" "$@"
        SINK_PASSWORD=$1
        shift
        ;;

      -s|-skipConfirmation)
        SKIP_CONF="enabled"
        ;;

      *)
        echo "ERROR: unknown option ${opt}"
        usage
    esac
  done
}

#######################################
# For a given search pattern, checks that there is a unique file matching that pattern
#   and return that unique file.
# Globals:
#   FOUND_FILE the unique file that is found.
# Arguments:
#   The search pattern, e.g ./streaming/target/streaming-bundled*.jar
#######################################
function find_unique_file() {
  local file_count
  file_count=$(ls ${1} | wc -l)
  if [[ file_count -ne 1 ]]; then
    echo "ERROR: Expected to find a unique file at ${1}"
    echo "ERROR: But found ${file_count}"
    exit 1
  fi
  FOUND_FILE=$(ls ${1})
}

#######################################
# Shows a Yes/No prompt for the user to confirm that a pipeline should start.
# Globals:
#   SKIP_CONF if non-empty the confirmation is skipped.
#######################################
function confirm_proceed() {
  if [[ -z ${SKIP_CONF} ]]; then
    echo "Do you want to continue with running the pipeline (choose 1 or 2)?"
    select ans in "Yes" "No"; do
        case ${ans} in
            Yes ) break;;
            No ) echo "Exiting"; exit 1;;
        esac
    done
  fi
}

#######################################
# MAIN
#######################################

if [[ $# -eq 0 ]]; then
  usage
fi

process_options "$@"

if [[ -z ${OUTPUT_DIR} ]]; then
  echo "ERROR: Output directory (-o) should be provided."
  usage
fi

if [[ -e ${OUTPUT_DIR} ]]; then
  echo "ERROR: Output directory ${OUTPUT_DIR} already exist; please use a new directory."
  exit 1
fi
# This will fail the whole script if it fails (because of `set -e`).
mkdir ${OUTPUT_DIR}

if [[ -z ${STREAMING_JAR} ]]; then
  find_unique_file "./streaming/target/streaming-bundled*.jar"
  STREAMING_JAR="${FOUND_FILE}"
fi

if [[ -z ${BATCH_JAR} && -n ${ENABLE_BATCH} ]]; then
  find_unique_file "./batch/target/batch-bundled*.jar"
  BATCH_JAR="${FOUND_FILE}"
fi

common_params="\
  --fhirDebeziumConfigPath=${CONFIG_FILE} \
  --fhirServerUrl=${SOURCE_URL} \
  --outputParquetPath=${OUTPUT_DIR} \
  --fhirSinkPath=${SINK_PATH} \
  --sinkUserName=${SINK_USERNAME} \
  --sinkPassword=${SINK_PASSWORD}
"
streaming_command="java -cp ${STREAMING_JAR} org.openmrs.analytics.Runner \
  ${common_params} --secondsToFlushParquetFiles=${FLUSH_STREAMING}"

echo "About to run the streaming pipeline first:"
echo ""
echo -e "STREAMING:\n  ${streaming_command}"
echo ""

confirm_proceed

echo "Running the streaming pipeline; log file: ${STREAMING_LOG} "
nohup ${streaming_command} >> ${STREAMING_LOG} 2>&1 &
echo "Streaming pipeline PID: $!"

# Waiting for the pipeline to start and fetch start time.
start_time=""
until [[ -n ${start_time} ]]; do
  echo "Checking that streaming pipeline is started and listens on port 9033 ..."
  ret=0
  # Set `ret` to prevent curl failure failing the whole script.
  start_time=$(curl -s 'http://localhost:9033/eventTime/start' || ret=$?)
  if [[ -z ${start_time} && ${ret} -eq 0 ]]; then
    sleep 5
  fi
done
echo "The streaming pipeline started at ${start_time}"

if [[ -n ${ENABLE_BATCH} ]]; then
  # TODO: Add --resourceList to script after improving its interaction  with the --activePeriod.
  batch_command="java -cp ${BATCH_JAR} org.openmrs.analytics.FhirEtl ${common_params} \
    --secondsToFlushParquetFiles=${FLUSH_BATCH} --activePeriod=${PERIOD_START}_${start_time} \
    --jdbcModeEnabled --resourceList=Encounter,Observation"

  echo "About to run the batch pipeline now:"
  echo ""
  echo -e "BATCH:\n  ${batch_command}"
  echo ""

  confirm_proceed

  echo "Running the batch pipeline; log file: ${BATCH_LOG} "
  nohup ${batch_command} >> ${BATCH_LOG} 2>&1 &
  echo "Batch pipeline PID: $!"
fi
