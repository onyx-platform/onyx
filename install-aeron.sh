#!/bin/bash

set -x
set -e

if [ ! -e Agrona ]; then
  git clone https://github.com/real-logic/Agrona.git
  cd Agrona
  git checkout bccda2335c728709ff6b8c6f23e89cc208d70e85
  ./gradlew install -x test
fi

if [ ! -e Aeron ]; then
  git clone https://github.com/real-logic/Aeron.git
  cd Aeron
  git checkout 95330cd20898b2055bf334e8a25a20879dbd345a
  ./gradlew install -x test
fi
