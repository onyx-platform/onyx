#!/bin/bash

set -x
set -e

if [ ! -e Agrona ]; then
  git clone https://github.com/real-logic/Agrona.git
  cd Agrona
  git checkout 31d85ecfcbe1e3de38a0b5a095239d2ba973234b
  ./gradlew
fi

if [ ! -e Aeron ]; then
  git clone https://github.com/real-logic/Aeron.git
  cd Aeron
  git checkout e233ed01918421295506135e2851d5d2843d72c2
  ./gradlew
fi
