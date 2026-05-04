#!/usr/bin/env bash
# Utility: robust Parquet row-count with retry/back-off.
# Used by validation scripts.

set -euo pipefail

# retry_rowcount <path_glob> <expected> <parquet_cli_jar>
#   path_glob    – shell glob pointing to a Parquet folder (wildcards allowed,
#                  e.g. "…/*/Patient/").  All *.parquet files found recursively
#                  under matching directories are counted.
#   expected     – integer row count we expect to see.
#   parquet_cli_jar – full path to the parquet-cli-<version>-runtime.jar file
#                  built with -Plocal so that Hadoop is bundled inside it.
#
# Prints the final count on stdout.
#
# Note: parquet-cli (replacing the deprecated parquet-tools since 1.12.0) does
# not expose a dedicated rowcount command.  We use `parquet-cli meta <file>`
# which prints one "Row group N:  count: X  …" line per row-group, then sum the
# count fields with awk.

retry_rowcount() {
  local parquet_glob="$1"
  local expected="$2"
  local parquet_cli_jar="$3"

  # CI can override cadence through env vars
  local max_retries="${ROWCOUNT_MAX_RETRIES:-15}"
  local sleep_secs="${ROWCOUNT_SLEEP_SECS:-20}"

  local retries=0
  local final_count=0

  # Verify JAR exists
  if [[ ! -f "$parquet_cli_jar" ]]; then
    echo "E2E TEST ERROR: parquet-cli JAR not found at: $parquet_cli_jar" >&2
    echo "0"
    return
  fi

  while true; do
    # ── 1. Collect all .parquet files under every directory matching the glob
    #       and sum row counts from `parquet-cli meta`.
    final_count=0
    local had_error=false
    while IFS= read -r parquet_file; do
      local file_count
      file_count=$(java -jar "${parquet_cli_jar}" meta "${parquet_file}" 2>/dev/null \
        | awk '/Row group/ { for(i=1;i<=NF;i++) if ($i=="count:") sum += $(i+1) }
               END { print sum+0 }')
      if [[ -z "${file_count}" || ! "${file_count}" =~ ^[0-9]+$ ]]; then
        echo "E2E TEST ERROR: [${parquet_file}] parquet-cli meta returned '${file_count}'" \
             "(treating as 0)" >&2
        had_error=true
        file_count=0
      fi
      final_count=$((final_count + file_count))
    done < <(find ${parquet_glob} -name "*.parquet" 2>/dev/null)

    # ── 2. Success?
    if [[ "${final_count}" -eq "${expected}" ]]; then
      echo "${final_count}"
      return
    fi

    # ── 3. Give up?
    if [[ "${retries}" -ge "${max_retries}" ]]; then
      echo "${final_count}"
      return
    fi

    # ── 4. Sleep & retry
    retries=$((retries + 1))
    echo "E2E TEST: [${parquet_glob}] count=${final_count}, expected=${expected} — retry ${retries}/${max_retries} in ${sleep_secs}s" >&2
    sleep "${sleep_secs}"
  done
}
