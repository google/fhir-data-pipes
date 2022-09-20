#!/bin/bash

# This is a little tool for simplifying merging multiple dependabot RRs into one
# as a workaround for the following feature request:
# https://github.com/dependabot/dependabot-core/issues/1190
# This is not meant to be fully automated and the final cherry-pick may require
# user intervention to resolve conflicts.

git fetch upstream
for b in $(git branch -r --list upstream/dependabot/*) ;
do
  echo "Fetching last commit from $b"
  c="$c $(git log HEAD~1..HEAD $b | awk '/^commit/{ print $2; exit;}')"
  #git cherry-pick $c
done
echo "Cherry-picking commits: $c"
git cherry-pick $c
