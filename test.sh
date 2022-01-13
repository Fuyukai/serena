#!/usr/bin/env bash

pytest --verbose --strict-markers
pytest --verbose --strict-markers -m 'slow' --cov-append
