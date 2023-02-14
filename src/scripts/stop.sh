#!/bin/bash

# change cwd
cd "$(dirname "$0")"

clear \
  && docker compose down \
  && echo "Done!"