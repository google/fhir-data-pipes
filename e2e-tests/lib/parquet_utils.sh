#!/usr/bin/env bash
# Utility: robust Parquet row-count with retry/back-off.
# Used by validation scripts.

set -euo pipefail

# retry_rowcount <path> <expected>  <parquet_jar>
#   path         – shell glob pointing to a Parquet folder (wildcards allowed).
#                  The glob is passed verbatim to parquet-tools, which understands
#                  Hadoop-style wild-cards (e.g. "…/*/Patient/").
#   expected     – integer row count we expect to see.
#   parquet_jar  – full path to the parquet-tools JAR file.
#
# Prints the final count on stdout.


: "${PARQUET_TOOLS:=parquet-tools}"

# Gate for "output exists" (dir contains at least one .parquet or a _SUCCESS)
: "${WAIT_READY_TOTAL_SECS:=120}"      # total wait time before we give up (seconds)
: "${WAIT_READY_SLEEP_SECS:=5}"        # poll interval (seconds)



: "${WAIT_READY_RESOURCES:=Patient,patient_flat}"
: "${PARQUET_UTILS_VERBOSE:=1}"


log_pu() {  # level, message...
  [ "${PARQUET_UTILS_VERBOSE}" = "1" ] || return 0
  printf '[parquet_utils][%s] %s\n' "$1" "$*" >&2
}

# Best-effort barrier: wait until a dataset dir has any data file or a _SUCCESS marker.
wait_for_output_ready() {
  local base="$1"
  local waited=0
  log_pu info "waiting for output: $base"
  while :; do

    if compgen -G "$base/*.parquet" >/dev/null || compgen -G "$base/_SUCCESS" >/dev/null; then
      log_pu info "output ready: $base"
      return 0
    fi
    if [ "$waited" -ge "$WAIT_READY_TOTAL_SECS" ]; then
      log_pu warn "timeout waiting for output: $base"
      return 1
    fi
    sleep "$WAIT_READY_SLEEP_SECS"
    waited=$(( waited + WAIT_READY_SLEEP_SECS ))
  done
}

# Decide if the given path should get the readiness gate (based on WAIT_READY_RESOURCES and path)
_should_gate() {
  local glob="$1"
  local list lc
  lc="$(printf '%s' "$WAIT_READY_RESOURCES" | tr '[:upper:]' '[:lower:]')"
  list=",$lc,"

  # Check for Patient (raw) gate
  if [[ "$glob" == *"/Patient/"* ]] && [[ "$list" == *",patient,"* ]]; then
    return 0
  fi
  # Check for patient_flat (view) gate
  if [[ "$glob" == *"/patient_flat/"* ]] && [[ "$list" == *",patient_flat,"* ]]; then
    return 0
  fi
  return 1
}

retry_rowcount() {
  local parquet_glob="$1"
  local expected="$2"
  local parquet_tools_jar="$3"

  # CI can override cadence through env vars
  local max_retries="${ROWCOUNT_MAX_RETRIES:-15}"
  local sleep_secs="${ROWCOUNT_SLEEP_SECS:-20}"

  local retries=0
  local raw_count=0
  local final_count=0

  # Verify JAR exists
  if [[ ! -f "$parquet_tools_jar" ]]; then
    echo "E2E TEST ERROR: parquet-tools JAR not found at: $parquet_tools_jar" >&2
    echo "0"
    return
  fi

# Best-effort readiness gate for the flakiest targets (Patient/patient_flat).
  if _should_gate "$parquet_glob"; then
    wait_for_output_ready "$parquet_glob" || log_pu warn "proceeding without readiness after timeout for: $parquet_glob"
  fi

  while true; do

   local err_file
   err_file="$(mktemp -t parquet_rowcount_err.XXXXXX)"
   # Using the jar directly keeps parity with existing behavior.
   local out_line
   out_line="$(java -Xms16g -Xmx16g -jar "${parquet_tools_jar}" rowcount "${parquet_glob}" 2>"$err_file" || true)"

   # ── 1. Ask parquet-tools for a row count
   raw_count="$(awk '{print $3}' <<<"$out_line" | tr -d '[:space:]')"

    # ── 2. Normalise raw_count
    if [[ -z "${raw_count}" || ! "${raw_count}" =~ ^[0-9]+$ ]]; then
      # Provide smarter diagnostics now that we have stderr.
      if [ -s "$err_file" ]; then
        if grep -qiE 'No such file|FileNotFound|No files|not a Parquet file' "$err_file"; then
          log_pu warn "parquet-tools reported: $(tr '\n' ' ' <"$err_file")"
        else
          log_pu info "parquet-tools stderr: $(tr '\n' ' ' <"$err_file")"
        fi
      fi
      # Also hint if the directory currently has no files.
      if ! compgen -G "$parquet_glob/*.parquet" >/dev/null && ! compgen -G "$parquet_glob/_SUCCESS" >/dev/null; then
        log_pu warn "no files visible under: $parquet_glob"
      fi

      echo "E2E TEST ERROR: [${parquet_glob}] parquet-tools returned '${raw_count}' (treating as 0)" >&2
      final_count=0
    else
      final_count="${raw_count}"
    fi
    rm -f "$err_file"

    # ── 3. Success?
    if [[ "${final_count}" -eq "${expected}" ]]; then
      echo "${final_count}"
      return
    fi

    # ── 4.Optional Fast-fail if no files ever matched on the *first* pass -- this can be implemented in future


    # ── 5. Give up?
    if [[ "${retries}" -ge "${max_retries}" ]]; then
      echo "${final_count}"
      return
    fi

    # ── 6. Sleep & retry
    retries=$((retries + 1))
    echo "E2E TEST: [${parquet_glob}] raw=${raw_count}, expected=${expected} — retry ${retries}/${max_retries} in ${sleep_secs}s" >&2
    sleep "${sleep_secs}"
  done
}
