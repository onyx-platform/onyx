#!/usr/bin/env bash

if [ -z "$CIRCLE_BRANCH" ]; then
	export BR=$CI_BRANCH
else
	export BR=$CIRCLE_BRANCH
fi

ARTIFACT_DIR=$CIRCLE_BUILD_NUM/$CIRCLE_NODE_INDEX/$BR"_"$1

grep -v _test log_artifact/$ARTIFACT_DIR/stderrout.log | grep onyx | grep Reflection

if [ $? -eq 1 ]; then
  exit 0
else
  echo "Failed reflection check"
fi
