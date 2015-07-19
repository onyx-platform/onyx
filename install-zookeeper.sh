#!/bin/bash

set -x
set -e

if [ ! -e zookeeper ]; then
  wget http://mirrors.ibiblio.org/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
  tar -xvf zookeeper-3.4.6.tar.gz
fi

cp resources/zoo.cfg zookeeper-3.4.6/conf/zoo.cfg
