#!/bin/bash
# Copyright 2022 Google LLC
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

# This is a little tool for simplifying merging multiple dependabot PRs into one
# as a workaround for the following feature request:
# https://github.com/dependabot/dependabot-core/issues/1190
# This is not meant to be fully automated and the final cherry-pick may require
# user intervention to resolve conflicts.

n=0
git fetch upstream --prune
for b in $(git branch -r --list upstream/dependabot/*) ;
do
  echo "Merging remote branch $b"
  git merge --no-edit $b
  ((n=$n+1))
done
echo "DONE merging ${n} branches"
