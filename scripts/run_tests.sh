#!/usr/bin/env bash
# Run the full test suite (unit, integration, stress) via CTest.
#   scripts/run_tests.sh              # all tests
#   scripts/run_tests.sh -L unit      # only one label: unit | integration | stress
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD_DIR="${BUILD_DIR:-build}"
ctest --test-dir "$BUILD_DIR" --output-on-failure "$@"
