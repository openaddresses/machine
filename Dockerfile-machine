FROM openaddr/prereqs:7

# From chef/openaddr/recipes/default.rb
COPY . /usr/local/src/openaddr
RUN cd /usr/local/src/openaddr && \
    pip3 install -U .
