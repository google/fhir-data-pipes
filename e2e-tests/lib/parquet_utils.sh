#!/usr/bin/env bash
# Utility: robust Parquet row-count with retry/back-off.
# Used by validation scripts.

set -euo pipefail

# retry_rowcount <path> <expected> <label>
#   path      – shell glob pointing to a Parquet folder (wildcards allowed).
#               The glob is passed verbatim to parquet-tools, which understands
#               Hadoop-style wild-cards (e.g. "…/*/Patient/").
#   expected  – integer row count we expect to see.
#   label     – short metric name for log messages.
#
# Prints the final count on stdout.
# Returns 0 if expected count is reached; returns 1 after retries if not.

retry_rowcount() {
  local parquet_glob="$1"
  local expected="$2"
  local label="$3"

  # CI can override cadence through env vars
  local max_retries="${ROWCOUNT_MAX_RETRIES:-5}"
  local sleep_secs="${ROWCOUNT_SLEEP_SECS:-5}"

  local retries=0
  local raw_count=0
  local final_count=0

  while true; do
    # ── 1. Ask parquet-tools for a row count
    raw_count=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
                "${parquet_glob}" 2>/dev/null | awk '{print $3}')

    # ── 2. Normalise raw_count
    if [[ -z "${raw_count}" || ! "${raw_count}" =~ ^[0-9]+$ ]]; then
      echo "E2E TEST ERROR: [${label}] parquet-tools returned '${raw_count}' " \
           "(treating as 0)" >&2
      final_count=0
    else
      final_count="${raw_count}"
    fi

    # ── 3. Success?
    if [[ "${final_count}" -eq "${expected}" ]]; then
      echo "${final_count}"
      return 0
    fi

    # ── 4.Optional Fast-fail if no files ever matched on the *first* pass -- this can be implemented in future


    # ── 5. Give up?
    if [[ "${retries}" -ge "${max_retries}" ]]; then
      echo "${final_count}"
      return 1
    fi

    # ── 6. Sleep & retry
    retries=$((retries + 1))
    echo "E2E TEST: [${label}] raw=${raw_count}, expected=${expected} — retry ${retries}/${max_retries} in ${sleep_secs}s" >&2
    sleep "${sleep_secs}"
  done
}