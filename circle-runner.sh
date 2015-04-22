#!/bin/bash

set -e

i=0
files=""

TEST_NSES=$(find test -name "*.clj" |sed s/test\\///|sed s/\\//\\./g|sed s/".clj$"//|sed s/"_"/"-"/g|tr '\n' ' '|sort)

echo "Testing " $TEST_NSES

for file in $TEST_NSES
do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    files+=" $file"
  fi
  ((++i))
done

export TEST_TRANSPORT_IMPL=$1 
lein with-profile dev,circle-ci midje $files
