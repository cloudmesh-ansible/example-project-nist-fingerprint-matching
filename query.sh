#!/usr/bin/env bash

source /etc/profile

ZK="{{ zookeeper_nodes | join(',') }}"

sqlline \
    -f /tmp/drill.sql \
    -u jdbc:drill:zk=$ZK;schema=hbase
