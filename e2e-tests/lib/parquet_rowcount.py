#!/usr/bin/env python3
# Copyright 2020-2026 Google LLC
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

"""Count total rows across all Parquet files matching a shell glob pattern.

Usage:
    python3 parquet_rowcount.py <glob_pattern>

The glob pattern may contain wildcards, e.g.:
    /workspace/e2e-tests/controller-spark/dwh/*/Patient/

Prints a single integer (the total row count) to stdout.
Prints 0 if no Parquet files are found; stderr carries a WARNING in that case.
Exits with code 1 only on argument errors.
"""

import glob
import os
import sys

import pyarrow.parquet as pq


def count_rows(glob_pattern: str) -> int:
    total = 0
    matched_any = False
    for path in glob.glob(glob_pattern):
        if os.path.isdir(path):
            # Explicitly collect non-empty files; pq.ParquetDataset raises ArrowInvalid
            # on 0-byte files that the pipeline may not have finished writing yet.
            ready_files = [
                os.path.join(root, f)
                for root, _, files in os.walk(path)
                for f in files
                if f.endswith(".parquet") and os.path.getsize(os.path.join(root, f)) > 0
            ]
            if ready_files:
                dataset = pq.ParquetDataset(ready_files)
                total += sum(fragment.metadata.num_rows for fragment in dataset.fragments)
                matched_any = True
            else:
                print(
                    f"WARNING: no non-empty Parquet files found in directory: {path}",
                    file=sys.stderr,
                )
        elif path.endswith(".parquet"):
            if os.path.getsize(path) > 0:
                total += pq.read_metadata(path).num_rows
                matched_any = True
            else:
                print(f"WARNING: skipping 0-byte Parquet file: {path}", file=sys.stderr)
    if not matched_any:
        print(f"WARNING: no Parquet files found for pattern: {glob_pattern}", file=sys.stderr)
    return total


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <glob_pattern>", file=sys.stderr)
        sys.exit(1)
    print(count_rows(sys.argv[1]))

