#!/bin/bash

set -e

cd packages/spreadsheet

echo "Running performance benchmarks..."

# Run all benchmarks and save results
go test -bench=. -benchmem ./... > /tmp/bench.txt

echo "Performance test results saved to /tmp/bench.txt"
echo "Results:"
cat /tmp/bench.txt

