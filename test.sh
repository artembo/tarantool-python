#!/bin/sh

set -exu # Strict shell (w/o -o pipefail)

# Install tarantool.
curl -L https://tarantool.io/installer.sh | VER=2.4 bash

# Install testing dependencies.
pip install -r requirements.txt
pip install pyyaml dbapi-compliance==1.15.0

# Run tests.
python setup.py test
