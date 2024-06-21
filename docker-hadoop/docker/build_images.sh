#!/bin/bash

# create base hadoop cluster docker image
docker build -f base/Dockerfile -t irm/hadoop-cluster-base:latest base

# create master node hadoop cluster docker image
docker build -f master/Dockerfile -t irm/hadoop-cluster-master:latest master
