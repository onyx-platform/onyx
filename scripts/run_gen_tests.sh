#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

TEST_NSES_GENERATIVE=$(find test -name "*.clj" |sed s/test\\///|sed s/\\//\\./g|sed s/".clj$"//|sed s/"_"/"-"/g | grep gen | tr '\n' ' '|sort)
TEST_CHECK_FACTOR=$1 lein test :only $TEST_NSES_GENERATIVE
