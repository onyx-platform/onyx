#!/usr/bin/env zsh

# Ok to reflect in tests
# Try not to worry about upstream libraries for now
# Only care about reflection, not boxed math for now
lein with-profile dev,reflection-check,circle-ci,clojure-1.8 test :smoke |& 
grep -v _test | 
grep onyx |
grep Reflection

if [ $? -eq 1 ]; then
  exit 0
else
  echo "Failed reflection check"
fi
