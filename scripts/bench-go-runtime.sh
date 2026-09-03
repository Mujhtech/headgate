#!/usr/bin/env bash
# Isolate JSON and allocator changes on the same Go 1.27 toolchain.
set -euo pipefail
cd "$(dirname "$0")/../go"
out=${1:-$(mktemp -d "${TMPDIR:-/tmp}/headgate-runtime-bench.XXXXXX")}
mkdir -p "$out"
out=$(cd "$out" && pwd)

GOEXPERIMENT=nojsonv2 go test -c -o "$out/legacy-json" .
GOEXPERIMENT=nosizespecializedmalloc go test -c -o "$out/previous-allocator" .
GOEXPERIMENT= go test -c -o "$out/default" .
for variant in legacy-json previous-allocator default; do
  : > "$out/$variant.txt"
done
# Interleave samples to reduce drift; never measure competing processes together.
for sample in {1..10}; do
  for variant in legacy-json previous-allocator default; do
    "$out/$variant" -test.run='^$' \
      -test.bench='Benchmark(DecodeArgs1K|TypedDispatch1K|MarshalArgs1K)$' \
      -test.benchmem -test.benchtime=200ms -test.cpu=1 >> "$out/$variant.txt"
  done
  echo "completed sample $sample/10"
done
if command -v benchstat >/dev/null 2>&1; then
  for baseline in legacy-json previous-allocator; do
    benchstat "$out/$baseline.txt" "$out/default.txt" > "$out/$baseline-vs-default.txt"
    cat "$out/$baseline-vs-default.txt"
  done
fi
echo "Benchmark artifacts: $out"
