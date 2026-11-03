#!/usr/bin/env bash
# Run the benchmark suite and write CSV + Markdown into docs/benchmark_results/.
# Extra arguments are passed to concurrentcore-bench (see --help), e.g.
#   scripts/run_benchmarks.sh --repetitions 5
#   scripts/run_benchmarks.sh --quick
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD_DIR="${BUILD_DIR:-build}"
BENCH="$BUILD_DIR/bin/concurrentcore-bench"
[ -x "$BENCH" ] || [ -x "$BENCH.exe" ] || { echo "build first: scripts/build.sh" >&2; exit 1; }
mkdir -p docs/benchmark_results
"$BENCH" --csv docs/benchmark_results/benchmark_results.csv \
         --markdown docs/benchmark_results/benchmark_report.md "$@"
