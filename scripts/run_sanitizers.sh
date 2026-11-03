#!/usr/bin/env bash
# Build and test with sanitizers using Clang.
#   scripts/run_sanitizers.sh address   # ASan + UBSan (Linux, macOS, Windows/llvm-mingw)
#   scripts/run_sanitizers.sh thread    # TSan (Linux / macOS only)
set -euo pipefail
cd "$(dirname "$0")/.."
KIND="${1:-address}"
case "$KIND" in
  address) SAN="address;undefined"; DIR=build-asan ;;
  thread)  SAN="thread";            DIR=build-tsan ;;
  *) echo "usage: $0 address|thread" >&2; exit 2 ;;
esac
GENERATOR=()
if command -v ninja >/dev/null 2>&1; then GENERATOR=(-G Ninja); fi
cmake -S . -B "$DIR" "${GENERATOR[@]}" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_COMPILER="${CC:-clang}" -DCMAKE_CXX_COMPILER="${CXX:-clang++}" \
  -DCC_SANITIZER="$SAN" -DCC_BUILD_BENCHMARKS=OFF
cmake --build "$DIR" -j
export ASAN_OPTIONS="${ASAN_OPTIONS:-halt_on_error=1}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-print_stacktrace=1:halt_on_error=1}"
export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=1:second_deadlock_stack=1}"
ctest --test-dir "$DIR" --output-on-failure
