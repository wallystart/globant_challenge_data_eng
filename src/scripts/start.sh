#!/bin/bash

./scripts/build.sh

docker-compose -f docker-compose.yml up -d
