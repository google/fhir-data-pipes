#!/usr/bin/env bash
# Utility: robust Parquet row-count with retry/back-off.
# Source from validation scripts.

set -euo pipefail

# retry_rowcount <globs> <expected> <label>
#   globs     – colon-separated shell globs to Parquet folders
#   expected  – integer row count we expect to see
#   label     – metric name for log messages
#
# Prints the final count on stdout.
# Returns 0 if expected count is reached; 1 otherwise.

retry_rowcount() {
  local globs="$1"
  local expected="$2"
  local label="$3"

  # Allow CI to override retry cadence without touching code
  local max_retries=${ROWCOUNT_MAX_RETRIES:-5}
  local sleep_secs=${ROWCOUNT_SLEEP_SECS:-5}

  local retries=0
  local raw_count=0
  local final_count=0

  IFS=':' read -r -a paths <<<"${globs}"

  while true; do
    raw_count=0

    # ── 1. Find a path that actually contains files
    for p in "${paths[@]}"; do
      shopt -s nullglob
      local files=( "${p}" )
      shopt -u nullglob

      if [[ ${#files[@]} -gt 0 ]]; then
        raw_count=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
                    "${p}" 2>/dev/null | awk '{print $3}')
        break
      fi
    done

    # ── 2. Normalise raw_count
    if [[ -z "${raw_count}" || ! "${raw_count}" =~ ^[0-9]+$ ]]; then
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