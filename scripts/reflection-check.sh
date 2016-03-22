#!/usr/bin/env bash

grep -v _test stderrout.log | grep onyx | grep Reflection

if [ $? -eq 1 ]; then
  exit 0
else
  echo "Failed reflection check"
fi
