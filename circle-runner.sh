#!/bin/bash

set -e


if [ -z "$CIRCLE_BRANCH" ]; then
	export BR=$CI_BRANCH
else
	export BR=$CIRCLE_BRANCH
fi

if [ $BR == "master" ]; then
	export TEST_CHECK_FACTOR=5
fi

i=0
files=""

TEST_NSES_NONGEN=$(find test -name "*.clj" |sed s/test\\///|sed s/\\//\\./g|sed s/".clj$"//|sed s/"_"/"-"/g|grep -v gen | tr '\n' ' '|sort)
TEST_NSES_GENERATIVE=$(find test -name "*.clj" |sed s/test\\///|sed s/\\//\\./g|sed s/".clj$"//|sed s/"_"/"-"/g | grep gen | tr '\n' ' '|sort)

for file in $TEST_NSES_NONGEN
do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    files+=" $file"
  fi
  ((++i))
done

# Run generative tests on all nodes to more evently distribute run time
# If they're taking too long we can reduce the TEST_CHECK_FACTOR
files+=" "$TEST_NSES_GENERATIVE

echo "Running " $files

export TEST_TRANSPORT_IMPL=$1 
lein with-profile dev,circle-ci midje $files
