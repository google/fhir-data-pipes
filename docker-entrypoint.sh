#!/bin/bash
# Copyright 2023 Google LLC
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

#######################################
# Enables jemalloc as the default memory allocator if it's available, otherwise the default glibc
# allocator is used. jemalloc is added to avoid the memory leak created by the glibc allocator.
# Refer https://github.com/google/fhir-data-pipes/issues/777 for details.
#######################################
enable_jemalloc() {
  JEMALLOC_PATH="/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so"
  JEMALLOC_FALLBACK="/usr/lib/x86_64-linux-gnu/libjemalloc.so"
  if [ -f "$JEMALLOC_PATH" ]; then
    export LD_PRELOAD=$LD_PRELOAD:$JEMALLOC_PATH
  elif [ -f "$JEMALLOC_FALLBACK" ]; then
    export LD_PRELOAD=$LD_PRELOAD:$JEMALLOC_FALLBACK
  else
    if [ "$JEMALLOC_PATH" = "$JEMALLOC_FALLBACK" ]; then
      MSG_PATH=$JEMALLOC_PATH
    else
      MSG_PATH="$JEMALLOC_PATH and $JEMALLOC_FALLBACK"
    fi
    echo "WARNING: attempted to load jemalloc from $MSG_PATH but the library couldn't be found. glibc will be used instead."
  fi
}

enable_jemalloc

# The -Xmx value is to make sure there is a minimum amount of memory; it can be
# increased if more memory is avaialble and is desired to be used by pipelines.
# Note this is retald to memory config in the above flink-conf.yaml too.
java -Xms6g -Xmx6g -jar /app/controller.jar
