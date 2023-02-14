#!/bin/bash

# change cwd
cd "$(dirname "$0")"

# status
clear \
  && docker compose ps