#!/usr/bin/env bash

set -eu pipefail

pytest --verbose --strict-markers
pytest --verbose --strict-markers -m 'slow' --cov-append
