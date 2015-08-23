#!/bin/bash

set -e

git config --global user.email "michael.drogalis@onyxplatform.org"
git config --global user.name "OnyxBot"
git clone $1 && \
    cd $2 && \
    git checkout master && \
    lein voom freshen && \
    git commit -am "CircleCI automatic commit: Freshening dependencies to latest." && \
    git push origin master && \
    cd ..
