#!/bin/bash -ex

mkdir -p /tmp/P
cp openaddr/VERSION /tmp/P/FULL
cut -f1 -d. /tmp/P/FULL > /tmp/P/MAJOR

docker pull ubuntu:16.04
aws s3 cp --quiet s3://data.openaddresses.io/docker/openaddr-prereqs-`cat /tmp/P/MAJOR`.tar.gz /tmp/img && gunzip -c /tmp/img | docker load || true

docker build -f Dockerfile-prereqs -t openaddr/prereqs:`cat /tmp/P/MAJOR` .
docker build -f Dockerfile-machine -t openaddr/machine:`cat /tmp/P/MAJOR` .
