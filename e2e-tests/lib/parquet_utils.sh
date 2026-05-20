#!/usr/bin/env bash
# Utility: robust Parquet row-count with retry/back-off.
# Used by validation scripts.

set -euo pipefail

# retry_rowcount <path_glob> <expected>
#   path_glob – shell glob pointing to a Parquet folder (wildcards allowed,
#               e.g. "…/*/Patient/").  All Parquet files found under matching
#               directories are counted via pyarrow (parquet_rowcount.py).
#   expected  – integer row count we expect to see.
#
# Prints the final count on stdout.
#
# Requires: python3 with pyarrow installed (pip3 install pyarrow).

# Resolve the directory containing this script so we can locate
# parquet_rowcount.py regardless of the caller's working directory.
_PARQUET_UTILS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_PARQUET_ROWCOUNT_PY="${_PARQUET_UTILS_DIR}/parquet_rowcount.py"

if ! python3 -c "import pyarrow" 2>/dev/null; then
  echo "ERROR: pyarrow is not installed. Run: pip3 install pyarrow" >&2
  return 1 2>/dev/null || exit 1
fi

_debug_list_glob() {
  local parquet_glob="$1"
  echo "E2E DEBUG: Listing entries matching glob: ${parquet_glob}" >&2
  local expanded
  # Use bash glob expansion; nullglob prevents literal output on no match
  shopt -s nullglob
  expanded=( ${parquet_glob} )
  shopt -u nullglob
  if [[ ${#expanded[@]} -eq 0 ]]; then
    echo "E2E DEBUG:   (no directories/files matched the glob)" >&2
  else
    for entry in "${expanded[@]}"; do
      echo "E2E DEBUG:   matched: ${entry}" >&2
      if [[ -d "${entry}" ]]; then
        # List parquet files with sizes; limit output to avoid flooding logs
        local file_count
        file_count=$(find "${entry}" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
        echo "E2E DEBUG:     parquet file count: ${file_count}" >&2
        find "${entry}" -name "*.parquet" 2>/dev/null \
          | head -20 \
          | while read -r f; do
              local sz
              sz=$(stat -c%s "${f}" 2>/dev/null || stat -f%z "${f}" 2>/dev/null || echo "?")
              echo "E2E DEBUG:       ${f}  (${sz} bytes)" >&2
            done
        if [[ "${file_count}" -gt 20 ]]; then
          echo "E2E DEBUG:       ... and $((file_count - 20)) more parquet files" >&2
        fi
      fi
    done
  fi
}

retry_rowcount() {
  local parquet_glob="$1"
  local expected="$2"

  # CI can override cadence through env vars
  local max_retries="${ROWCOUNT_MAX_RETRIES:-15}"
  local sleep_secs="${ROWCOUNT_SLEEP_SECS:-20}"

  local retries=0
  local raw_count=""
  local final_count=0

  # Show what the glob resolves to before the first attempt
  _debug_list_glob "${parquet_glob}"

  while true; do
    # ── 1. Count rows via pyarrow
    local py_err
    py_err=$(mktemp)
    raw_count=$(python3 "${_PARQUET_ROWCOUNT_PY}" "${parquet_glob}" 2>"${py_err}") || true
    local py_stderr_msg
    py_stderr_msg=$(cat "${py_err}" 2>/dev/null); rm -f "${py_err}"

    echo "E2E DEBUG: [${parquet_glob}] pyarrow raw_count='${raw_count}'" \
         "${py_stderr_msg:+| Python stderr: ${py_stderr_msg}}" >&2

    # ── 2. Normalise
    if [[ -z "${raw_count}" || ! "${raw_count}" =~ ^[0-9]+$ ]]; then
      echo "E2E TEST ERROR: [${parquet_glob}] parquet_rowcount.py returned '${raw_count}'" \
           "(treating as 0)${py_stderr_msg:+; Python error: ${py_stderr_msg}}" >&2
      final_count=0
    else
      final_count="${raw_count}"
    fi

    # ── 3. Success?
    if [[ "${final_count}" -eq "${expected}" ]]; then
      echo "${final_count}"
      return
    fi

    # ── 4. Give up?
    if [[ "${retries}" -ge "${max_retries}" ]]; then
      # On final failure, re-list the glob so we can see the state at timeout
      echo "E2E DEBUG: Final attempt failed; re-listing glob at timeout:" >&2
      _debug_list_glob "${parquet_glob}"
      echo "${final_count}"
      return
    fi

    # ── 5. Sleep & retry
    retries=$((retries + 1))
    echo "E2E TEST: [${parquet_glob}] raw=${raw_count}, expected=${expected} — retry ${retries}/${max_retries} in ${sleep_secs}s" >&2
    sleep "${sleep_secs}"
  done
}
