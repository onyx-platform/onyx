#!/usr/bin/env zsh

grep -v _test log_artifact/$ARTIFACT_DIR/stderrout.log | 
grep onyx |
grep Reflection

if [ $? -eq 1 ]; then
  exit 0
else
  echo "Failed reflection check"
fi
