#!/bin/bash

git config --global user.email "michael.drogalis@onyxplatform.org"
git config --global user.name "OnyxBot"
git clone $1 && cd $2 && git checkout master && lein voom freshen && git diff --exit-code

rc=$?;

if [[ $rc != 0 ]]; then
  git commit -am "CircleCI automatic commit: Freshening dependencies to latest." && git push origin master
fi
