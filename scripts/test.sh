#!/bin/bash

set -e

cd packages/spreadsheet

go test ./...

go test -coverprofile=/tmp/coverage.out -v && \
  go tool cover -html=/tmp/coverage.out -o /tmp/coverage.html && \
  open /tmp/coverage.html

