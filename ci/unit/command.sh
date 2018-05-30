#!/bin/bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex

export PATH=$PATH:$PWD/vendor/jruby/bin
gem install bundler
cd ../plugins/this
cp /usr/share/logstash/logstash-core/versions-gem-copy.yml /usr/share/logstash/logstash-core-plugin-api/versions-gem-copy.yml
bundle install --path /usr/share/logstash/vendor/bundle
bundle exec rspec -fd --pattern spec/**/*_spec.rb,spec/**/*_specs.rb