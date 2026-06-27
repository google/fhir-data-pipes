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

retry_rowcount() {
  local parquet_glob="$1"
  local expected="$2"

  # CI can override cadence through env vars
  local max_retries="${ROWCOUNT_MAX_RETRIES:-15}"
  local sleep_secs="${ROWCOUNT_SLEEP_SECS:-20}"

  local retries=0
  local raw_count=""
  local final_count=0

  while true; do
    # ── 1. Count rows via pyarrow
    local py_err
    py_err=$(mktemp)
    raw_count=$(python3 "${_PARQUET_ROWCOUNT_PY}" "${parquet_glob}" 2>"${py_err}") || true
    local py_stderr_msg
    py_stderr_msg=$(cat "${py_err}" 2>/dev/null); rm -f "${py_err}"

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
      echo "${final_count}"
      return
    fi

    # ── 5. Sleep & retry
    retries=$((retries + 1))
    echo "E2E TEST: [${parquet_glob}] raw=${raw_count}, expected=${expected} — retry ${retries}/${max_retries} in ${sleep_secs}s" >&2
    sleep "${sleep_secs}"
  done
}
