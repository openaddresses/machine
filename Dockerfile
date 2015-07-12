FROM ubuntu:14.04

# Install python
RUN \
  apt-get update && \
  apt-get install -y python python-dev python-pip python-virtualenv

# Install machine prerequirements
RUN apt-get install -y python-cairo python-gdal python-pip python-dev libpq-dev

# Set up and configure machine
ADD . /machine
WORKDIR /machine
RUN pip install -U .

# Default entrypoint
ENTRYPOINT ["openaddr-process-one"]